import os

from pyspark.sql.session import SparkSession
from pyspark.ml.feature import VectorAssembler


spark = SparkSession \
    .builder \
    .appName("CustomerProfiling") \
    .getOrCreate()

historic_data = spark \
    .read \
    .csv(os.path.curdir + '/preprocessed_data/static_data_v1.csv', header=True, inferSchema=True)
# historic_data.summary().show()
historic_data.printSchema()

vecAssembler = VectorAssembler(inputCols=["REGION", "COUNTY"], outputCol="features")
new_df = vecAssembler.transform(historic_data)
new_df.show()

for column in new_df.columns:
    new_df = new_df.withColumnRenamed(column, column.lower())

new_df.show()

new_df.write.format("org.apache.spark.sql.cassandra") \
    .mode('append') \
    .options(table="static_data", keyspace="customer_profiling") \
    .save()
