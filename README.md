# Gruppe 10

## Data integration

### Prerequisites

* Python 3.7
* Docker 19.03+
* Docker-Compose 1.24.1
* Pipenv 2018.11.26

## Setup

1. Clone this repository to an empty directory on your computer.
2. Activate the virtual environment with `pipenv shell` and install the dependencies from the Pipfile with `pipenv install --dev`.
3. Copy the `sample.env` file to `.env` file and fill in the password.
4. Start the database service in docker with `docker-compose up -d`.