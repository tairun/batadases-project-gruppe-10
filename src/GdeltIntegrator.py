import pandas as pd
from src.DataIntegrator import DataIntegrator

from typing import List, Generator


class GdeltIntegrator(DataIntegrator):
    """
    Class description.
    """

    def __init__(self):
        self.gdelt_headers = ["GLOBALEVENTID", "SQLDATE", "MonthYear", "Year", "FractionDate", "Actor1Code", "Actor1Name",
                              "Actor1CountryCode", "Actor1KnownGroupCode", "Actor1EthnicCode", "Actor1Religion1Code",
                              "Actor1Religion2Code", "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code", "Actor2Code",
                              "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode", "Actor2EthnicCode",
                              "Actor2Religion1Code", "Actor2Religion2Code", "Actor2Type1Code", "Actor2Type2Code",
                              "Actor2Type3Code", "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode", "QuadClass",
                              "GoldsteinScale", "NumMentions", "NumSources", "NumArticles", "AvgTone", "Actor1Geo_Type",
                              "Actor1Geo_FullName", "Actor1Geo_CountryCode", "Actor1Geo_ADM1Code", "Actor1Geo_Lat",
                              "Actor1Geo_Long", "Actor1Geo_FeatureID", "Actor2Geo_Type", "Actor2Geo_FullName",
                              "Actor2Geo_CountryCode", "Actor2Geo_ADM1Code", "Actor2Geo_Lat", "Actor2Geo_Long",
                              "Actor2Geo_FeatureID", "ActionGeo_Type", "ActionGeo_FullName", "ActionGeo_CountryCode",
                              "ActionGeo_ADM1Code", "ActionGeo_Lat", "ActionGeo_Long", "ActionGeo_FeatureID", "DATEADDED",
                              "SOURCEURL"]
        self.gdelt_actor1 = [self.gdelt_headers[i]
                             for i in [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 38]]
        self.gdelt_actor2 = [self.gdelt_headers[i]
                             for i in [15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 45]]
        self.gdelt_event_geo = [self.gdelt_headers[i] for i in
                                [35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55]]
        self.gdelt_event_action = [self.gdelt_headers[i] for i in [
            25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 5, 15, 52]]
        self.gdelt_eventid_and_date = [
            self.gdelt_headers[i] for i in [57, 26, 0, 1, 2, 3, 4]]
        self.gdelt_data_management_fields = [
            self.gdelt_headers[i] for i in [56, 57]]

    def read_csv(self, file_path: str, limit: int = None) -> Generator:
        """
        Generetor that yields lines of a file.
        """
        with open(file_path, mode="r") as f:
            if limit:
                data = pd.read_csv(f, nrows=limit, sep="\t",
                                   names=self.gdelt_headers)
            else:
                data = pd.read_csv(
                    f, sep="\t", names=self.gdelt_headers, encoding="utf8")

            for row in data.iterrows():
                yield row

    def insert_actor1(self, row):
        # TODO: implement SQL Parser
        print(row[self.gdelt_actor1])
        print("xd")

    def wrapper_read(self, file_path):
        for row in self.read_csv(file_path, 5):
            self.insert_actor1(row)


if __name__ == "__main__":
    integrator = GdeltIntegrator()
    file = "raw_data/20191027.export.CSV"
    integrator.wrapper_read(file)
