
# I. Purpose of this database in the context of the startup, Sparkify, and their analytical goals.

- A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. So we need a database which stores transformed data so that the analytics team can do aggregration easily on it.       

- In this project, we use EMR + AWS S3 to build Data Lake and we need to pull raw data stored on S3. By using EMR, we can make use of a big data cluster computing performance to process data parallelly.      

- We have 2 stages: 1. we need to load song_data from S3, transform it then write into S3 in the parquet format. 2. we need to load log_data then join with the song_data from parquet files in S3 that we ever processed in the previous step to get the well formatted songplays_data.    

- So as to take advantage of a EMR cluster for parallel processing, we need to use PySpark to run our code. All of these PySpark codes are in the file "etl.py". This file will serve as a full-flow data pipeline to extract data from S3, transform then load the results back to S3 in partitioned parquet. 

- This database's design is Star Schema with 1 fact: songplays and 4 dimensional tables: users, songs, artists, time.


# II. How to run the Python scripts
**Notes:**    
Before starting to run below scripts, we need to create EMR cluster + 1 S3 bucket to store output data.   
After creating, we need to update the dwh.cfg file with related key + secrete to work with AWS Services.   
     

- We need to run 1 script: 

    Run **etl.py** to run ETL pipeline to copy data from JSON files in S3 to EMR cluster, transform then ingest them back to S3 bucket in parquet format.    
    In the terminal, then type: `python etl.py`

# III. An explanation of the files in the repository

1. etl.py: Reads and processes files from song_data and log_data and loads them into your EMR tables. You can fill this out based on your work in the ETL notebook.   

2. README.md provides discussion on your project.   

# IV. State and justify your database schema design and ETL pipeline.

- From my point of view, our data is not to big and there aren't many relationships between each entities so Star Schema is good and simple enough for analysts to perform analytics queries.  

- The dim tables are well drilled down and contains all required fields to provide info for the fact tables. 

- The fact table shows the aggregated information about songs, artists, users so that the manager of the company Sparkify can have a broad overview about what their users do and how they interact with the app. 

- From my point of view, this project exercise is pretty practical and good for beginners when working with Spark and Big Data Cluster. While working with EMR, we not only cares for coding but also monitor the performance and cost that our pipeline takes. Really cool experience!!!