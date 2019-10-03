from pyspark import SparkContext
from pyspark.sql import Column, DataFrame, SparkSession, SQLContext, functions
from pyspark.sql.functions import *
from py4j.java_collections import MapConverter
from delta.tables import *
import shutil
import threading

# Clear previous run's delta-tables
try:
    shutil.rmtree("/tmp/delta-table")
except:
    pass

# Create SparkContext
sc = SparkContext()
sqlContext = SQLContext(sc)

# Enable SQL for the current spark session.
spark = SparkSession \
    .builder \
    .appName("utilities") \
    .master("local[*]") \
    # config io.delta.sql.DeltaSparkSessionExtension -- to enable custom Delta-specific SQL commands
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    # config parallelPartitionDiscovery.parallelism -- control the parallelism for vacuum
    .config("spark.sql.sources.parallelPartitionDiscovery.parallelism","8") \
    .getOrCreate()

# Apache Spark 2.4.x has a known issue (SPARK-25003) that requires explicit activation
# of the extension and cloning of the session. This will unnecessary in Apache Spark 3.x.
if spark.sparkContext.version < "3.":
    spark.sparkContext._jvm.io.delta.sql.DeltaSparkSessionExtension() \
        .apply(spark._jsparkSession.extensions())
    spark = SparkSession(spark.sparkContext, spark._jsparkSession.cloneSession())

# Create a table
print("########### Create a Parquet table ##############")
data = spark.range(0, 5)
data.write.format("parquet").save("/tmp/delta-table")

# Convert to delta
print("########### Convert to Delta ###########")
DeltaTable.convertToDelta(spark, "parquet.`/tmp/delta-table`")

# Read the table
df = spark.read.format("delta").load("/tmp/delta-table")
df.show()

deltaTable = DeltaTable.forPath(spark, "/tmp/delta-table")
print("######## Vacuum the table ########")
deltaTable.vacuum()

print("######## Describe history for the table ######")
deltaTable.history().show()

# SQL Vacuum
print("####### SQL Vacuum #######")
spark.sql("VACUUM '%s' RETAIN 169 HOURS" % "/tmp/delta-table").collect()

# SQL describe history
print("####### SQL Describe History ########")
print(spark.sql("DESCRIBE HISTORY delta.`%s`" % ("/tmp/delta-table")).collect())

# cleanup
shutil.rmtree("/tmp/delta-table")
