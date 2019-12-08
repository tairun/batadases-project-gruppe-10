from EurostatIntegrator import EurostatIntegrator


class TourismIntegrator(EurostatIntegrator):
    def __init__(self):
        super().__init__()
        self.data = "./raw_data/tour_occ_nim_1_Data.csv"

        self.headers = ["Time", "Geo", "RESID" "Unit",
                        "Accommodation", "Value", "Flags"]

        self.table_names = ["country", "tourist"]

        self.tables = {
            "country": {
                "headers": [self.headers[i] for i in [1]],
                "attributes": ["Geo_Name"],
                "uniques": ["Geo_Name"]
            },
            "tourist": {
                "headers": [self.headers[i] for i in [1, 5, 0, 2, 4, 3]],
                "attributes": ["CID", "Value", "Time", "RESID", "Accommodation", "Unit"],
                "uniques": []
            }
        }


if __name__ == "__main__":
    integrator = TourismIntegrator()
    table_names = ["tourist"]

    _ = input(f"Press 'Enter' to start the integration process ...")
    integrator.execute_script(integrator.table_script)  # Create the tables.
    integrator.insert_wrapper(
        integrator.data, headers=integrator.headers, table_names=table_names)
