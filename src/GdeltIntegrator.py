import pandas as pd
from DataIntegrator import DataIntegrator

from typing import List, Generator


class GdeltIntegrator(DataIntegrator):
    """
    Class description.
    """

    def read_csv(self, file_path: str, limit: int = None) -> Generator:
        """
        Generetor that yields lines of a file.
        """
        with open(file_path, mode="r") as f:
            if limit:
                data = pd.read_csv(f, nrows=limit, sep="\t")
            else:
                data = pd.read_csv(f, sep="\t")

            print(data.head(2))

            for row in data.iterrows():
                yield row


if __name__ == "__main__":
    integrator = GdeltIntegrator()
    file = "raw_data/20191027.export.CSV"

    [print(row) for row in integrator.read_csv(file, 5)]
