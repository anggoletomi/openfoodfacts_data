# OpenFoodFacts Data Pipeline

This repository demonstrates a **data engineering project** showcasing various tools. The pipeline ingests data from the [Open Food Facts API](https://world.openfoodfacts.org/data).

## Table of Contents
1. [Tech Stack & Workflow](#tech-stack--workflow)
2. [Project Steps](#project-steps)
3. [Metabase Dashboard](#metabase-dashboard)
4. [How to Run Locally](#how-to-run-locally)

## Tech Stack & Workflow
Below is a simplified view of end-to-end flow:

1. **Open Food Facts API** : Fetch pages of product data by search term.
2. **MongoDB (NoSQL)** : Store data fetched from the OpenFoodFacts API. Upsert new and existing records into a MongoDB Atlas collection.
3. **AWS S3** : Dump the entire MongoDB collection to Amazon S3 as a JSON file.
4. **Snowflake** : Create or update a Snowflake warehouse/database/schema, ingest JSON from S3, and store it in a **RAW** table.
5. **dbt** : SQL transformations, modeling, and testing within Snowflake. Create staging (`stg_`) models, and final tables (`fact_nutrition`, `dim_product`) in Snowflake.
6. **Metabase** : BI tool for visual analytics and dashboards. Point Metabase to Snowflake, build dashboards to visualize.
7. **GitHub Actions** : CI/CD pipeline that automates scripts & dbt runs on push or schedule. Pipeline file: `off_ci.yml`
8. **Docker (for Metabase)** : Containerizes Metabase for local deployment.

## Project Steps
Below is a high-level description of each script and stage.

1. **`openfoodfacts_api.py`**  
   - **Goal**: Fetch multiple pages of products from Open Food Facts for specified search terms.
   - **Actions**:  
     - Connect to the API  
     - Insert or upsert data into MongoDB (`DB_NAME.COLLECTION_NAME`)

2. **`export_to_s3.py`**  
   - **Goal**: Export the entire MongoDB collection to S3 as a JSON.  
   - **Actions**:  
     - Read from MongoDB  
     - Dump to JSON  
     - Upload to `S3_BUCKET` with a timestamped filename

3. **`setup_snowflake.py`**  
   - **Goal**: Prepare & load data in Snowflake.  
   - **Actions**:  
     - Create or use Warehouse/DB/Schema  
     - Create or truncate RAW table  
     - Create a Snowflake stage pointing to S3  
     - Copy the latest JSON file into `RAW_OPENFOODFACTS`  
     - Validate row count

4. **`run_dbt.py`**  
   - **Goal**: Orchestrate dbt transformations & tests.  
   - **Actions**:  
     - `dbt run`: Build staging views (`stg_openfoodfacts`), dimension (`dim_product`), and fact (`fact_nutrition`) models  
     - `dbt test`: Run data quality checks, e.g. `not_null`, unique keys, and relationships

5. **Metabase**  
   - **Goal**: Visual analytics and dashboards.  
   - **Actions**:  
     - Connect to Snowflake.  
     - Build charts (bar charts, scatter plots, etc.) using columns like `Sugars_100G`, `Fat_100G`, etc.  
     - Possibly combine multiple filters to make the dashboard interactive.

6. **GitHub Actions**  
   - **Goal**: Automated CI/CD.  
   - **Actions**:  
     - Run a pipeline (`off_ci.yml`) that:  
       - Installs Python & dependencies  
       - Executes scripts to refresh data  
       - Launches dbt transformations & tests

---

## Metabase Dashboard
We use a **Docker-based** Metabase instance. The typical steps to get Metabase up:

1. **Install Docker** locally.  
2. **Run**: `docker run -d -p 3000:3000 --name metabase metabase/metabase`  
3. **Access** Metabase at `http://localhost:3000`  
4. **Configure** Snowflake connection

> **Note**  
> Metabase does **not** support "public" dashboards like Tableau Public. In this repo, you'll find example screenshots in the `/metabase` folder.

---

## How to Run Locally
1. **Clone** this repo:
   ```bash
   git clone https://github.com/YourName/openfoodfacts_data.git
   cd openfoodfacts_data
   ```

2. **Create a `.env`** file with your credentials (MongoDB, AWS, Snowflake):
   ```bash
   MONGO_URI="mongodb+srv://..."
   AWS_ACCESS_KEY_ID="..."
   AWS_SECRET_ACCESS_KEY="..."
   AWS_S3_BUCKET="..."
   SNOWFLAKE_ACCOUNT="..."
   SNOWFLAKE_USER="..."
   SNOWFLAKE_PASSWORD="..."
   SNOWFLAKE_ROLE="..."
   SNOWFLAKE_WAREHOUSE_NAME="..."
   SNOWFLAKE_DATABASE_NAME="..."
   SNOWFLAKE_SCHEMA_NAME="..."
   ```

3. **Set up a Python environment**, e.g.:
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

4. **Export & run** the pipeline scripts:
   ```bash
   python src/openfoodfacts_api.py
   python src/export_to_s3.py
   python src/setup_snowflake.py
   python src/run_dbt.py
   ```

5. **Launch Metabase** (via Docker):
   ```bash
   docker run -d -p 3000:3000 --name metabase metabase/metabase
   # Access http://localhost:3000 in your browser
   ```

<br/><br/>

> **Author**: _anggoletomi_  
> **Contact**: [LinkedIn](https://www.linkedin.com/in/anggoletomi/) / [GitHub](https://github.com/anggoletomi)  