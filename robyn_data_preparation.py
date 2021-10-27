import pandas as pd
import numpy as np
import requests
import logging
import datetime
import argparse
import gevent
from random import randint
import os
import yaml


class ConnectorAuthenticationError(Exception):
    """An error indicating that an external API (such as Google) returns
    a response indicating that the user token/authentication from the
    onboarding form is broken
    """

    pass


def read_config(client_name):
    filepath = os.path.join(os.getcwd(), client_name, "config.yml")
    with open(filepath) as config:
        config = yaml.safe_load(config)
    return config


class DataHandler:
    def __init__(
        self,
        json_url,
        user,
        config,
        file_name="train_dataset_for_robyn.csv",
    ):
        self.config = config
        self.json_url = json_url
        self.user = user
        self.logger = logging.basicConfig(level=logging.INFO)
        self.columns_to_group_by = ["sourceMedium", config["colnames"].get('date_col'), "campaign", "type"]
        self.date_column = config["colnames"].pop("date_col")
        self.file_name = file_name
        self.campaign_type_dict = {
            "Retargeting": "Retargeting",
            "Prospecting": "Prospecting",
            "No-Targeting": "No-Targeting",
        }
        self.columns_mapping = {
            config["colnames"].get("totalcost_col"): "S",
            config["colnames"].get("impressions_col"): "I",
            config["colnames"].get("clicks_col"): "C",
        }
        # Remove None keys if exist
        self.columns_mapping = {
            k: v for k, v in self.columns_mapping.items() if k is not None
        }

    @staticmethod
    def split_campaign(x, split_by=""):
        try:
            return x.split(split_by)[1]
        except:
            return None

    def _verify_response(self, response, is_retry: bool = False):
        if response.status_code == 401:
            data = response.json()
            error_msg = data.get("message")
            raise ConnectorAuthenticationError(error_msg)
        elif not response.ok:
            if not is_retry and response.status_code == 429:
                self.logger.info("Sleeping and trying again")
                gevent.sleep(5 + randint(0, 5))

                with requests.Session() as session:
                    response = session.send(response.request)

                return self._verify_response(response, is_retry=True)

            self.logger.error(
                "Something went wrong while getting data. "
                "Status code: %s - %s"
                % (
                    response.status_code,
                    response.text,
                )
            )
            raise Exception(
                "Something went wrong while getting data. "
                "Status code: %s - %s"
                % (
                    response.status_code,
                    response.json()["message"],
                )
            )
        return response.json()

    def get_data_from_url(self):
        res = requests.get(self.json_url)
        data = self._verify_response(res)
        data = pd.DataFrame(data["data"])
        return data

    def get_total_revenue(self, df):
        # get total revenue for all sources
        df_total_revenue = (
            df.groupby(self.date_column)[self.config["colnames"]["revenue_col"]]
            .sum()
            .reset_index(name="total_revenue")
        )

        # Group by high-level fields
        df = df.groupby(self.columns_to_group_by).sum().reset_index()

        df = pd.merge(df, df_total_revenue, on=self.date_column)

        return df

    def get_fb_impressions_df(self, df, fb=None):
        # self.config['facebook_source_medium']
        if self.config.get("facebook_split") :
            index_columns = list(
                set(self.columns_to_group_by + [self.date_column, "total_revenue"])
            )
            df_fb = (
                df[df.sourceMedium == fb]
                .set_index(index_columns)
                .unstack(["sourceMedium"])
            )
            if len(df_fb) > 0:
                df_fb.columns = [
                    "{}_{}".format(t, v[0].upper()) for v, t in df_fb.columns
                ]
                df_fb = df_fb.unstack(["campaign"])
                df_fb.columns = ["{}_{}".format(v, t) for v, t in df_fb.columns]
                return df_fb
        else:
            return None

    def get_costs_df(self, df, fb=None):
        if self.config.get("facebook_split"):
            df = df.loc[df.sourceMedium != fb, :]

        columns = [self.date_column, "total_revenue", "sourceMedium"] + [
            v
            for k, v in self.config["colnames"].items()
            if v != self.config["colnames"]["revenue_col"]
        ]
        df_others = (
            df[columns]
            .groupby([self.date_column, "total_revenue", "sourceMedium"])
            .sum()
            .unstack(["sourceMedium"])
        )
        df_others.columns = [
            "{}_{}".format(t, self.columns_mapping.get(v)) for v, t in df_others.columns
        ]

        return df_others

    def transform_date_column(self, df):
        try:
            df[self.date_column] = df[self.date_column].dt.date
        except:
            df[self.date_column] = df[self.date_column].apply(
                lambda x: datetime.datetime.fromtimestamp(x / 1000)
            )
            df[self.date_column] = df[self.date_column].dt.date
        return df

    def rename_campaign_type(self, df):

        if "facebook_campaign_type_dict" and "split_campaigns_by" in self.config:
            df.campaign = df.campaign.apply(
                lambda x: self.split_campaign(
                    x, split_by=self.config["split_campaigns_by"]
                )
            )
            type_dict = self.config["facebook_campaign_type_dict"]
            for tp in set(df.campaign.dropna()):
                if tp not in type_dict.keys():
                    type_dict[tp] = "Other"
            df.campaign = df.campaign.map(type_dict)
            df.campaign.fillna("None", inplace=True)

        return df

    def source_filter(self, df):
        # Select sources from config file
        if "source_medium" in self.config:
            mask = df.sourceMedium.apply(lambda x: x in self.config["source_medium"])
            df = df.loc[mask, :]
        return df

    def data_preparation(self, df):

        df = self.rename_campaign_type(df)
        df = self.get_total_revenue(df)
        df = self.source_filter(df)

        df.sourceMedium = df.sourceMedium.str.replace("-", "_")

        df_fb = self.get_fb_impressions_df(
            df, fb=self.config.get("facebook_source_medium")
        )
        df_others = self.get_costs_df(df, fb=self.config.get("facebook_source_medium"))
        if df_fb is not None:
            result = pd.merge(df_others, df_fb, left_index=True, right_index=True)
        else:
            result = df_others

        result.fillna(0, inplace=True)
        result = result.loc[:, (result != 0).any(axis=0)]
        result.reset_index(inplace=True)
        result = self.transform_date_column(result)
        result.set_index(self.date_column, inplace=True)

        return result

    def save_df_to_file(self, df):
        filepath = os.path.join(os.getcwd(), self.user, self.file_name)
        df.to_csv(filepath)


def main(arguments, config):

    DH = DataHandler(
        json_url=arguments["json_url"], user=arguments["user"], config=config
    )
    df = DH.get_data_from_url()
    result = DH.data_preparation(df)
    DH.save_df_to_file(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--json-url",
        required=True,
        help="URL for the json data from database",
    )
    parser.add_argument(
        "--user",
        required=True,
        help="User name",
    )

    parsed, unknown = parser.parse_known_args()
    arguments = vars(parsed)
    config = read_config(arguments["user"])

    main(arguments, config)
