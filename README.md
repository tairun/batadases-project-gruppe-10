# Gruppe 10

## Data integration

### Prerequisites

* Python 3.7.x
* Docker 19.03+
* Docker-Compose 1.24.1+
* Pipenv 2018.11.26+

### Setup

1. Clone this repository to an empty directory on your machine and `cd` into it.
2. Activate the virtual environment with `pipenv shell` and install the dependencies from the Pipfile with `pipenv install --dev`.
3. Copy the `sample.env` file to `.env` file and fill in the password.
4. Start the database service in docker with `docker-compose up -d`.
5. You can go to `localhost:8080` to inspect the database with the browser.
6. **`WARNING`**: The GDELTv1.0 data set needs alot of space on the disk. For the timespan *2015/01/01* to *2019/12/31* at least **125GB** of free space is required.
7. Execute the main method in `app.py`. The execution context needs to be set to the root of the git repository.

### SQL dump

1. Open a shell to the database docker container: ` docker exec -it <container-name> /bin/ash`
2. Use the `pg_dump` command to create a database dump: `pg_dump -U db-proj -d db-proj-hs19 -f /db_dump/dump_`date +%d-%m-%Y"_"%H_%M_%S`.sqldump`
3. The `/dump` folder from the container is mounter locally in the repository as `./db_dump/`, where you can find the created sql dump.

**Restore dump:** Copy the dump into the `./db_dump/` direcotry. Spawn a shell in the container and run the following command: `psql <dbname> < /dump/<dumpfile>`

### Shortcuts taken

- The 'Income' and 'Tourist' dataset were manually modified in the following way:
  - Removed spaces (functioning as thousands delimiter) in between numbers
  - Removed ':' as empty value marker
  - Removed flags 'u', 'e' and 'b' in value column
- Lifted some foreign key restraints which will be added later via 'ALTER TABLE'
- Adjusted the datatypes in the schema to suit data better

### Results

The Gdelt dataset is the largest of the 3 datasets, containing  approx. 170 Mio rows for 5 years worth of data. The data is split over 1'800 files (one for each day since Jan 1st, 2015). Extrapolating the time needed to insert all the data with a single threaded python application, results in a value close to 450 hours. We designed the code in such a way that it would allow multi-threaded insertation (on a per file bases). Assuming 10 threads, theoretically this would result in a runtime of still ~ 45 hours for the insertion alone. **In the end, we did no manage to integrate most of the data**.  

We are interested to know if there are any bottlenecks in the integration process, e.g.
- Docker / Filesystem
- Unoptimized query string
- Unnecessary commint in database
- Python loops and chunksizes when inserting
- etc.

We will further investigate this.

| Data Source |   Total Rows | Rows Inserted | Insert Duration [mm:ss] | Colums |
| ----------- | -----------: | ------------: | ----------------------: | -----: |
| GDelt       | ~170'000'000 |        95'300 |                   16:34 |     58 |
| Income      |        7'680 |         7'680 |                   00:13 |      9 |
| Tourism     |      442'800 |       442'800 |                   14:07 |      7 |