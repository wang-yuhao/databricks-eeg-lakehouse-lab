# Databricks notebook source
# MAGIC %md
# MAGIC # Day 15: Data Source Connectors
# MAGIC 
# MAGIC ## Professional Certification - Advanced Data Engineering
# MAGIC 
# MAGIC **Learning Objectives:**
# MAGIC - Master JDBC connectivity for relational databases
# MAGIC - Configure cloud storage connectors (ADLS, S3, GCS)
# MAGIC - Implement NoSQL database connectors (MongoDB, Cassandra)
# MAGIC - Understand Kafka/Event Hub streaming connectors
# MAGIC - Explore Delta Sharing connector patterns
# MAGIC - Configure external data sources with Unity Catalog
# MAGIC 
# MAGIC **Exam Relevance:**
# MAGIC - Data Processing & Transformation: 25%
# MAGIC - Production & Operations: 20%
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Import Libraries and Configure Environment

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import os

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: JDBC Connectors for Relational Databases
# MAGIC 
# MAGIC ### Key Concepts:
# MAGIC - Driver configuration
# MAGIC - Connection pooling
# MAGIC - Partition strategies
# MAGIC - Query pushdown optimization

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1: PostgreSQL JDBC Connection

# Configuration for PostgreSQL
jdbc_url = "jdbc:postgresql://your-server.postgres.database.azure.com:5432/your_db"
connection_properties = {
    "user": dbutils.secrets.get(scope="db-secrets", key="postgres-user"),
    "password": dbutils.secrets.get(scope="db-secrets", key="postgres-password"),
    "driver": "org.postgresql.Driver",
    "ssl": "true",
    "sslmode": "require"
}

# Read with partition strategy for parallel processing
df_postgres = (spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "medical_records")
    .option("user", connection_properties["user"])
    .option("password", connection_properties["password"])
    .option("driver", connection_properties["driver"])
    .option("numPartitions", 8)  # Parallel reads
    .option("partitionColumn", "patient_id")  # Column to partition on
    .option("lowerBound", 1)
    .option("upperBound", 1000000)
    .load())

print(f"PostgreSQL records loaded: {df_postgres.count()}")
df_postgres.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2: SQL Server JDBC with Query Pushdown

# SQL Server configuration
sqlserver_url = "jdbc:sqlserver://your-server.database.windows.net:1433;database=EEG_DB"

# Custom query with predicate pushdown
custom_query = """
(SELECT 
    patient_id,
    recording_date,
    eeg_channel,
    sample_rate,
    duration_seconds
 FROM eeg_recordings
 WHERE recording_date >= '2024-01-01'
   AND quality_flag = 'VALID'
) AS filtered_data
"""

df_sqlserver = (spark.read
    .format("jdbc")
    .option("url", sqlserver_url)
    .option("dbtable", custom_query)
    .option("user", dbutils.secrets.get(scope="db-secrets", key="sqlserver-user"))
    .option("password", dbutils.secrets.get(scope="db-secrets", key="sqlserver-password"))
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("fetchsize", 10000)  # Optimize fetch size
    .load())

df_sqlserver.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3: MySQL with Connection Pooling

mysql_url = "jdbc:mysql://your-server.mysql.database.azure.com:3306/research_db"

# Connection pooling configuration
connection_props_mysql = {
    "user": dbutils.secrets.get(scope="db-secrets", key="mysql-user"),
    "password": dbutils.secrets.get(scope="db-secrets", key="mysql-password"),
    "driver": "com.mysql.cj.jdbc.Driver",
    "useSSL": "true",
    "requireSSL": "true",
    # Connection pooling
    "maxPoolSize": "20",
    "minPoolSize": "5",
    "maxIdleTime": "300"
}

df_mysql = (spark.read
    .format("jdbc")
    .option("url", mysql_url)
    .option("dbtable", "subject_metadata")
    .options(**connection_props_mysql)
    .load())

df_mysql.createOrReplaceTempView("subject_metadata")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Cloud Storage Connectors
# MAGIC 
# MAGIC ### Key Concepts:
# MAGIC - ADLS Gen2 with ABFS protocol
# MAGIC - S3 with s3a protocol
# MAGIC - GCS with gs protocol
# MAGIC - Service principal authentication
# MAGIC - Managed identity patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1: Azure Data Lake Storage (ADLS) Gen2

# Configure ADLS Gen2 with Service Principal
storage_account = "yourstorageaccount"
container = "eeg-raw-data"

spark.conf.set(
    f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
    "OAuth"
)
spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get(scope="azure-sp", key="client-id")
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get(scope="azure-sp", key="client-secret")
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{dbutils.secrets.get(scope='azure-sp', key='tenant-id')}/oauth2/token"
)

# Read EDF files from ADLS
adls_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/physionet/sleep-edf/"

# List files
dbutils.fs.ls(adls_path)

# Read binary EDF files
df_edf_binary = (spark.read
    .format("binaryFile")
    .option("pathGlobFilter", "*.edf")
    .option("recursiveFileLookup", "true")
    .load(adls_path))

print(f"EDF files found: {df_edf_binary.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2: AWS S3 Connector

# Configure S3 access with IAM role or access keys
spark.conf.set("fs.s3a.access.key", dbutils.secrets.get(scope="aws-keys", key="access-key"))
spark.conf.set("fs.s3a.secret.key", dbutils.secrets.get(scope="aws-keys", key="secret-key"))
spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
spark.conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Read CSV files from S3
s3_path = "s3a://your-bucket/data/processed/"

df_s3 = (spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(s3_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3: Google Cloud Storage (GCS)

# Configure GCS with service account
spark.conf.set("fs.gs.auth.service.account.enable", "true")
spark.conf.set(
    "fs.gs.auth.service.account.json.keyfile",
    "/dbfs/mnt/config/gcs-service-account.json"
)

# Read Parquet from GCS
gcs_path = "gs://your-bucket/processed-data/"

df_gcs = (spark.read
    .format("parquet")
    .load(gcs_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: NoSQL Database Connectors
# MAGIC 
# MAGIC ### Key Concepts:
# MAGIC - MongoDB connector configuration
# MAGIC - Cassandra connector patterns
# MAGIC - Azure Cosmos DB integration
# MAGIC - Document-to-tabular mapping

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1: MongoDB Connector

# MongoDB connection string
mongo_uri = f"mongodb://your-server.mongo.cosmos.azure.com:10255"
mongo_database = "eeg_research"
mongo_collection = "subject_metadata"

# Configure MongoDB Spark Connector
df_mongo = (spark.read
    .format("mongodb")
    .option("spark.mongodb.connection.uri", mongo_uri)
    .option("spark.mongodb.database", mongo_database)
    .option("spark.mongodb.collection", mongo_collection)
    .option("spark.mongodb.auth.source", "admin")
    .option("spark.mongodb.auth.username", dbutils.secrets.get(scope="mongo", key="username"))
    .option("spark.mongodb.auth.password", dbutils.secrets.get(scope="mongo", key="password"))
    .option("spark.mongodb.ssl", "true")
    .load())

df_mongo.printSchema()
df_mongo.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2: MongoDB Aggregation Pipeline Pushdown

# Complex aggregation pipeline pushed to MongoDB
pipeline = [
    {"$match": {"age": {"$gte": 18, "$lte": 65}}},
    {"$match": {"diagnosis": {"$in": ["insomnia", "sleep_apnea"]}}},
    {"$project": {
        "subject_id": 1,
        "age": 1,
        "diagnosis": 1,
        "recordings": 1,
        "_id": 0
    }}
]

df_mongo_filtered = (spark.read
    .format("mongodb")
    .option("spark.mongodb.connection.uri", mongo_uri)
    .option("spark.mongodb.database", mongo_database)
    .option("spark.mongodb.collection", mongo_collection)
    .option("pipeline", str(pipeline))
    .load())

print(f"Filtered MongoDB records: {df_mongo_filtered.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3: Azure Cosmos DB (SQL API)

# Cosmos DB configuration
cosmosdb_config = {
    "spark.cosmos.accountEndpoint": "https://your-cosmosdb.documents.azure.com:443/",
    "spark.cosmos.accountKey": dbutils.secrets.get(scope="cosmos", key="master-key"),
    "spark.cosmos.database": "eeg_database",
    "spark.cosmos.container": "recordings"
}

df_cosmos = (spark.read
    .format("cosmos.oltp")
    .options(**cosmosdb_config)
    .option("spark.cosmos.read.inferSchema.enabled", "true")
    .load())

df_cosmos.createOrReplaceTempView("cosmos_recordings")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Streaming Connectors
# MAGIC 
# MAGIC ### Key Concepts:
# MAGIC - Kafka connector patterns
# MAGIC - Azure Event Hubs integration
# MAGIC - Checkpoint management
# MAGIC - Watermarking strategies

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1: Apache Kafka Connector

kafka_bootstrap_servers = "your-kafka-broker.kafka.azure.net:9093"
kafka_topic = "eeg-realtime-stream"

# Kafka connection string with SASL
kafka_config = {
    "kafka.bootstrap.servers": kafka_bootstrap_servers,
    "subscribe": kafka_topic,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": f'''org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{dbutils.secrets.get(scope='kafka', key='connection-string')}";''',
    "startingOffsets": "latest",
    "failOnDataLoss": "false"
}

# Read streaming data from Kafka
df_kafka_stream = (spark.readStream
    .format("kafka")
    .options(**kafka_config)
    .load())

# Parse JSON payload
schema = StructType([
    StructField("patient_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("eeg_channel", StringType(), True),
    StructField("amplitude_uv", DoubleType(), True)
])

df_parsed = (df_kafka_stream
    .select(
        col("key").cast("string"),
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    )
    .select("data.*", "kafka_timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2: Azure Event Hubs Connector

eventhub_namespace = "your-eventhub-namespace"
eventhub_name = "eeg-events"

# Event Hubs connection string
eventhub_connection_string = dbutils.secrets.get(scope="eventhub", key="connection-string")

ehConf = {
    "eventhubs.connectionString": spark.sparkContext._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(eventhub_connection_string),
    "eventhubs.consumerGroup": "$Default",
    "eventhubs.startingPosition": '{"offset":"-1","seqNo":-1,"enqueuedTime":null,"isInclusive":true}',
    "eventhubs.maxEventsPerTrigger": 10000
}

df_eventhub_stream = (spark.readStream
    .format("eventhubs")
    .options(**ehConf)
    .load())

# Parse Event Hub body
df_eventhub_parsed = (df_eventhub_stream
    .withColumn("body_str", col("body").cast("string"))
    .withColumn("data", from_json(col("body_str"), schema))
    .select("data.*", "enqueuedTime", "offset"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Delta Sharing Connector
# MAGIC 
# MAGIC ### Key Concepts:
# MAGIC - Delta Sharing profile configuration
# MAGIC - Cross-org data sharing
# MAGIC - Unity Catalog integration

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1: Read from Delta Share

# Delta Sharing profile path
profile_path = "/dbfs/mnt/config/delta-sharing-profile.json"

# Read shared table
shared_table_url = f"{profile_path}#research_consortium.eeg_data.anonymized_recordings"

df_shared = (spark.read
    .format("deltaSharing")
    .option("responseFormat", "delta")
    .load(shared_table_url))

df_shared.show(5)
print(f"Shared records: {df_shared.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Unity Catalog External Locations
# MAGIC 
# MAGIC ### Key Concepts:
# MAGIC - External location registration
# MAGIC - Storage credential management
# MAGIC - Schema evolution with external tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1: Create External Location

# SQL to create external location
spark.sql("""
CREATE EXTERNAL LOCATION IF NOT EXISTS eeg_external_storage
URL 'abfss://external-data@yourstorage.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL azure_service_principal_credential)
COMMENT 'External EEG data storage for research partners'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2: Create External Table

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS catalog.schema.external_eeg_recordings
(
    recording_id STRING,
    patient_id STRING,
    recording_date DATE,
    channel STRING,
    sample_rate INT,
    file_path STRING
)
LOCATION 'abfss://external-data@yourstorage.dfs.core.windows.net/recordings/'
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Performance Optimization Techniques
# MAGIC 
# MAGIC ### Key Concepts:
# MAGIC - Connection pooling best practices
# MAGIC - Partition pruning
# MAGIC - Predicate pushdown
# MAGIC - Parallel reads configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1: Optimized JDBC Read with Partitioning

def read_jdbc_optimized(
    jdbc_url: str,
    table: str,
    partition_column: str,
    num_partitions: int = 8
) -> "DataFrame":
    """
    Read JDBC table with optimized partitioning strategy.
    
    Args:
        jdbc_url: JDBC connection URL
        table: Table name or SQL query
        partition_column: Column to partition on (must be numeric)
        num_partitions: Number of parallel partitions
    
    Returns:
        Spark DataFrame
    """
    # Get bounds for partition column
    bounds_query = f"(SELECT MIN({partition_column}) as min_val, MAX({partition_column}) as max_val FROM {table}) AS bounds"
    
    bounds_df = (spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", bounds_query)
        .option("user", connection_properties["user"])
        .option("password", connection_properties["password"])
        .load())
    
    bounds = bounds_df.collect()[0]
    lower_bound = bounds["min_val"]
    upper_bound = bounds["max_val"]
    
    # Read with calculated bounds
    df = (spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", connection_properties["user"])
        .option("password", connection_properties["password"])
        .option("numPartitions", num_partitions)
        .option("partitionColumn", partition_column)
        .option("lowerBound", lower_bound)
        .option("upperBound", upper_bound)
        .option("fetchsize", 10000)
        .load())
    
    return df

# Example usage
# df_optimized = read_jdbc_optimized(
#     jdbc_url=jdbc_url,
#     table="medical_records",
#     partition_column="patient_id",
#     num_partitions=16
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Error Handling and Retry Logic

from pyspark.sql.utils import AnalysisException
import time

def read_with_retry(
    format_type: str,
    options: dict,
    max_retries: int = 3,
    retry_delay: int = 5
) -> "DataFrame":
    """
    Read data with automatic retry on failure.
    
    Args:
        format_type: Data source format (jdbc, delta, parquet, etc.)
        options: Read options dictionary
        max_retries: Maximum number of retry attempts
        retry_delay: Delay between retries in seconds
    
    Returns:
        Spark DataFrame
    """
    for attempt in range(max_retries):
        try:
            df = spark.read.format(format_type).options(**options).load()
            print(f"Successfully read data on attempt {attempt + 1}")
            return df
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("Max retries reached. Raising exception.")
                raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Exam Tips
# MAGIC 
# MAGIC ### Key Takeaways:
# MAGIC 
# MAGIC 1. **JDBC Connectors:**
# MAGIC    - Always use partitioning for parallel reads
# MAGIC    - Configure appropriate fetch sizes
# MAGIC    - Use query pushdown when possible
# MAGIC    - Secure credentials with secret scopes
# MAGIC 
# MAGIC 2. **Cloud Storage:**
# MAGIC    - Use service principals for authentication
# MAGIC    - Configure appropriate protocols (abfss, s3a, gs)
# MAGIC    - Optimize for large file reads
# MAGIC 
# MAGIC 3. **NoSQL Databases:**
# MAGIC    - Push aggregations to source when possible
# MAGIC    - Handle document-to-tabular transformations
# MAGIC    - Configure connection pooling
# MAGIC 
# MAGIC 4. **Streaming Sources:**
# MAGIC    - Configure checkpoints for fault tolerance
# MAGIC    - Use watermarks for late data handling
# MAGIC    - Manage consumer groups/offsets
# MAGIC 
# MAGIC 5. **Delta Sharing:**
# MAGIC    - Understand profile configuration
# MAGIC    - Know sharing limitations
# MAGIC    - Integration with Unity Catalog
# MAGIC 
# MAGIC ### Exam Focus Areas:
# MAGIC - Connector configuration parameters
# MAGIC - Performance optimization techniques
# MAGIC - Security and authentication patterns
# MAGIC - Error handling strategies
# MAGIC - Integration with Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## Practice Exercises
# MAGIC 
# MAGIC 1. Configure a JDBC connection to read 10M records with optimal partitioning
# MAGIC 2. Set up Delta Sharing to share a table across workspaces
# MAGIC 3. Create a streaming pipeline from Event Hubs to Delta Lake
# MAGIC 4. Implement retry logic for unreliable data sources
# MAGIC 5. Configure external locations in Unity Catalog
# MAGIC 
# MAGIC ## Next Steps
# MAGIC - Day 16: Delta Sharing & Federation
# MAGIC - Day 17: Monitoring & Observability
# MAGIC - Day 18: PhysioNet Dataset Integration
