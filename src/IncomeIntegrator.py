#!/usr/bin/env python3

from src.EurostatIntegrator import EurostatIntegrator


class IncomeIntegrator(EurostatIntegrator):
    def __init__(self):
        super().__init__()
        self.data = "./raw_data/ilc_di15_1_Data.csv"

        self.headers = ["Year", "Geo", "Unit",
                        "Type", "Citizens", "Sex", "AgeGroup", "Value", "Flags"]

        self.table_names = ["country", "income"]

        self.tables = {
            "country": {
                "headers": [self.headers[i] for i in [1]],
                "attributes": ["Geo_Name"],
                "uniques": ["Geo_Name"]
            },
            "income": {
                # TODO: How to get CID
                "headers": [self.headers[i] for i in [1, 7, 2, 4, 6, 3, 0, 5]],
                "attributes": ["CID", "Value", "Unit", "Citizens", "AgeGroup", "Type", "Year", "Sex"],
                "uniques": []
            }
        }


if __name__ == "__main__":
    integrator = IncomeIntegrator()
    table_names = ["income"]

    _ = input(f"Press 'Enter' to start the integration process ...")
    integrator.execute_script(integrator.table_script)  # Create the tables.
    integrator.insert_wrapper(
        integrator.data, headers=integrator.headers, table_names=table_names)
