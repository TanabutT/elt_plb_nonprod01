be honest and as an expert data enginner,

reference to best practice,
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

