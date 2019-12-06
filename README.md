# Gruppe 10

## Data integration

### Prerequisites

* Python 3.7
* Docker 19.03+
* Docker-Compose 1.24.1
* Pipenv 2018.11.26

## Setup

1. Clone this repository to an empty directory on your machine and `cd` into it.
2. Activate the virtual environment with `pipenv shell` and install the dependencies from the Pipfile with `pipenv install --dev`.
3. Copy the `sample.env` file to `.env` file and fill in the password.
4. Start the database service in docker with `docker-compose up -d`.
5. You can go to `localhost:8080` to inspect the database with the browser.
6. **`WARNING`**: The GDELTv1.0 data set needs alot of space on the disk. For the timespan *2015/01/01* to *2019/12/31* at least **125GB** of free space is required.
7. Execute the main method in `app.py`. The execution context needs to be set to the root of the git repository.