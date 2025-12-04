plan for build folder structure only 
design it with best practice and suitable for detail of the task below as well
project for ELT with python airflow (gcp datacomposer in future) 
    - CI/CD : github action with python script will store in google cloud storage (gcs) separate bronze, silver, gold (anoter bucket)
    - schedule bacth run : daily
    - each run after first run will be incremental / CDC (change data capture) to reduce cost and make it faster
    - high level architecture is 3 zone :bronze to silver to gold
    bronze zone
        1.extract mssql 9 services database which have various number of table inside : this is data source
        2.extact data source by
            2.1 connenct to mssql server and extract data from each table
            2.2 convert data to parquet format
            2.3 connect to google cloud storage (gcs)
            2.3 store data in google cloud storage (gcs) with separate folder for each table assume to be daily 
                2.3.1 top folder will be extract date that run pipeline
                2.3.2 sub folder will be database name
                2.3.3 file name will be table name with out dbo_ prefix
        3.load from google cloud storage (gcs) and do not transform data from google cloud storage (gcs) to bigquery (bq) to be staging area
            3.1 connect to google cloud storage (gcs)
            3.2 connect to bigquery (bq)
            3.3 load data from google cloud storage (gcs) to be table in bigquery (bq) by each service database to be dataset
                3.3.1 dataset will be name as database name
                3.3.2 table will be name as filename from google cloud storage (gcs) make sure the name iswith out dbo_ prefix
    Silver zone
        4.transform data from staging area (bigquery) to data warehouse (bigquery) - not plan or design yet
            4.1 connect to bigquery (bq) staging area
            4.2 connect to bigquery (bq) data warehouse
            4.3 transform data from bigquery (bq) staging area to bigquery (bq) data warehouse
    Gold zone
        5.transform data from data warehouse (bigquery) to data mart (bigquery) - not plan or design yet
            5.1 connect to bigquery (bq) data warehouse
            5.2 connect to bigquery (bq) data mart
            5.3 transform data from bigquery (bq) data warehouse to bigquery (bq) data mart
                5.3.1 prerequisite is to have data warehouse
                5.3.2 prerequisite is to get format or requirement from business/data science team use case
                5.3.3 build data mart pipeline data warehouse in silver zone to be the structure that business/data science team can use
            5.4 future will have vector database to be used by business/data science team
                5.4.1 connect to bigquery (bq) data mart
                5.4.2 connect to vector database of google cloud service : https://cloud.google.com/vertex-ai/docs/vector-search 
                5.4.3 transform data from bigquery (bq) data mart to vector database
            
    Across zone above
        6.validation report
            6.1 Source to gogle cloud storage (SQL Server to Cloud Bucket File)
                * Purpose: To verify that the data extracted from SQL Server (E) is fully and correctly written to the intermediate file (L) in the cloud bucket.
                * Validation Check: Compare aggregate statistics or row counts between the SQL Server source and the generated Parquet file.
            6.2 Cloud Bucket File to BigQuery (L to S)
                    * Purpose: To verify that the data loaded from the file (T) has been correctly written to the final BigQuery table. This is the most crucial validation point.
                    * Validation Check: Compare data and aggregates between the Parquet file and the loaded BigQuery table.
            6.3 End-to-End (SQL Server to BigQuery Table)
                    * Purpose: A final, comprehensive check to ensure the entire ETL process worked.
                    * Validation Check: Compare aggregates (e.g., total row count, sum of key numeric columns, min/max dates) directly between the SQL Server source and the BigQuery target.
                    for large datasets, is:
                    Recommendation: 2.1 Query SQL Server vs. Query BigQuery (Aggregate Validation)
                    For high-level, quick, and critical validations (row count, key aggregates), directly querying the source and target is the most efficient method.
                    * The Format: You compare the results of simple, identical, or functionally equivalent SQL queries run against both databases.
                        * Source Query (SQL Server T-SQL):SQLSELECT COUNT(*) AS row_count, SUM(sale_amount) AS total_sales FROM dbo.source_table WHERE load_date = 'YYYY-MM-DD';
                        * 
                        * Target Query (BigQuery Standard SQL):SQLSELECT COUNT(*) AS row_count, SUM(sale_amount) AS total_sales FROM target_dataset.target_table WHERE load_date = 'YYYY-MM-DD';
                        * 
                        * Validation: Assert that row_count and total_sales are exactly equal on both sides.
            6.4 Silver to Gold validation - not plan or design yet

            6.4 validation report format
                The best format combines portability for other systems and readability for normal users.
                Recommendation: 3.2 Table in Target (BigQuery) + Visualization Tool
                The most practical and durable solution is a Hybrid Approach:
                Part A: BigQuery Validation Report Table (For Data Engineers & Automated Systems)
                * Format: A dedicated table in BigQuery (e.g., audit_dataset.etl_validation_log).
                * Content: This table acts as a log, storing the results of your checks:
                    * job_id (Airflow run ID)
                    * validation_step (e.g., 'Source_to_Staging_RowCount')
                    * source_value (e.g., 100000 - SQL Server count)
                    * target_value (e.g., 100000 - BigQuery count)
                    * status ('SUCCESS', 'FAILURE', 'WARNING')
                    * failure_details (e.g., 'Row counts mismatched by 5 rows.')
                * Pro: Highly portable, easily queryable for historical trends, and accessible to automated alerts (e.g., PagerDuty, Slack).
                Part B: External Report (For Normal Users/Stakeholders)
                Since the BigQuery table is hard for non-technical users, you should visualize the key metrics.
                * Recommended Format: A simple dashboard created using a tool that connects to BigQuery (e.g., Looker Studio, Tableau, Power BI).
                * Content:
                    * A Status Indicator (Green/Red) for the latest load.
                    * A chart showing Row Counts over time (for trend analysis).
                    * A table displaying key validation metrics (e.g., 'Total Sales Difference: $0.00', 'Missing Rows: 0').
                * Pro: Easy for a normal user to see and understand at a glance, requires no technical knowledge, and is highly consumable.

        7. Alert/ notification on 
            7.1 validation report
            7.2 pipeline status
            7.3 error report

        8. rerun pipeline
            8.1 run specific task
            8.2 run from failed task
            8.3 run from specific date
            8.4 run from specific database service (whole table inside)
            8.5 run from specific table (with dependency or related table)