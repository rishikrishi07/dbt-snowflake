# dbt + Snowflake + Airflow Integration

This repository demonstrates the integration of dbt with Snowflake, orchestrated using Apache Airflow.

## Project Structure

- `airflow/` - Airflow configuration and utilities
  - `dbt/` - dbt project files
    - `models/` - dbt models
    - `profiles.yml` - dbt connection profiles
- `dags/` - Airflow DAGs
  - `simple_dbt_test.py` - Example DAG using Airflow's connection to Snowflake

## Setup Instructions

### Prerequisites

- Docker
- Docker Compose
- Snowflake account

### Getting Started

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/dbt-snowflake-airflow.git
   cd dbt-snowflake-airflow
   ```

2. Set up environment variables:
   - Create a `.env` file in the root directory
   - Add Snowflake connection details

3. Start Airflow:
   ```
   docker compose up -d
   ```

4. Access Airflow:
   - Open a browser and navigate to `http://localhost:8080`
   - Username: `airflow`
   - Password: `airflow`

## Running dbt Models

The included DAG (`simple_dbt_test`) demonstrates how to:

1. Get Snowflake connection details from Airflow
2. Run a dbt debug command to verify the connection
3. Run a simple dbt model

## Development

This project uses:
- Airflow 2.10.5
- dbt-core 1.8.0
- dbt-snowflake 1.8.0

## License

MIT 