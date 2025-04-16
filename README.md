# Data Ingestion ETL Pipeline

This project provides a Python-based ETL pipeline that integrates user data with weather details and enriches it with a randomly generated company. The enriched data is stored in DuckDB using a normalized schema that supports queries across Users, Locations, Companies, and Weather.


## Features

- **Batch Pipeline:**  
  - Fetch 500 user records from [RandomUser API](https://randomuser.me/api/?results=500)
  - Retrieve current weather details (using [Open-Meteo API](https://api.open-meteo.com/v1/forecast)) by extracting geographic coordinates.
  - Enrich user records with weather and randomly generated company data.
  - Persist enriched data in DuckDB with a normalized schema.

- **Real-Time Production Design (Design Document):**  
  - An architecture built for a GCP environment focusing on low-latency ingestion, high availability, and fault tolerance.
  - Use of GCP services such as Pub/Sub, Dataflow, Cloud Run, and BigQuery.


## Setup Instructions

1. **Clone the repository:**

   ```bash
   git clone <repository_url>
   cd data_pipeline_project
   ```

2.	**Set up a Python virtual environment:**

    ```bash
    python3 -m venv venv # macOS/Linux
    source venv/bin/activate  

    # python -m venv venv # Windows
    # venv\Scripts\activate
    ```

3.	**Install dependencies:**

    ```bash
    python3 -m pip install -r requirements.txt # macOS/Linux
    python -m pip install -r requirements.txt # Windows
    ```

I will proceed using syntax for macOS/Linux since that's what I'm developing on, but the same format of ```python``` instead of ```python3``` command preceeding the arguements in the terminal can be used for Windows users.


## Running the Pipeline

1.	**Batch Pipeline (ETL):**

Run the main ETL pipeline script:

    ```bash
    python3 src/etl_pipeline.py
    ```

This script will:
	- Fetch user data.
	- Enrich it with weather and company information.
	- Store the enriched data in a DuckDB database file (enriched_data.duckdb).

2.	**Testing and Visualization:**

To inspect data at various stages, run the test script:

    ```bash
    python tests/test_etl_pipeline.py
    ```

The test script prints sample data from each pipeline stage (fetching, enrichment, and final data load into DuckDB).


## Querying the Data in DuckDB

### Tables Overview

Table Descriptions:
	- USERS: Contains basic user data including name, contact information, and foreign keys to the location and company.
	- LOCATIONS: Stores geographic coordinates.
	- COMPANIES: Contains randomly generated company details.
	- WEATHER: Stores the current weather details; each record is linked to a location.

You can use DuckDB’s Python API or the DuckDB CLI to run queries on the tables.

### Python Query

    ```python
    import duckdb
    conn = duckdb.connect('enriched_data.duckdb')
    df = conn.execute("SELECT * FROM users LIMIT 10").fetchdf()
    print(df)
    ```

### DuckDB CLI Query

1.	Open the Terminal in VS Code or your system terminal.
2.	Install DuckDB by running:

    ```bash
    brew install duckdb
    ```

Once the DuckDB CLI is installed, you can query the enriched_data.duckdb database in interactive mode or within one line. Below is an example of a one-liner query:

    ```bash
    duckdb enriched_data.duckdb "SELECT * FROM users LIMIT 10;"
    ```


## Project Design Documents

    - ERD: See design/ERD.md for the database Entity-Relationship Diagram in Mermaid code.
    - ERD .png Diagaram: See design/mermaid-etl-diagram.png for a visualization of the Mermaid diagram for batch ETL.
	- Real-Time Architecture: See design/realtime_architecture.md for the cloud-native streaming architecture design on GCP.
    - Real-Time Architecture .png Diagram: See design/mermaid-cloud-arch-diagram.png for a visualization of the Mermaid diagram for streaming architecture.


## Creating a Git Bundle

After committing all changes, you can create a Git bundle containing the entire repository ready for review with:

    ```bash
    git bundle create data_pipeline_project.bundle --all
    ```