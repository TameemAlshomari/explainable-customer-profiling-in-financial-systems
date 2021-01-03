import os

from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.linalg import Vectors
from pyspark.ml.pipeline import Pipeline
from pyspark.mllib.classification import StreamingLogisticRegressionWithSGD
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.session import SparkSession
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.types import Row, StringType
from pyspark.ml.feature import CountVectorizer
import pyspark.sql.functions as F


def convert_value_to_row(df: DataFrame):
    temp = df.rdd.map(lambda x: Vectors.dense(x.split(',')[:-1])).toDF(['features'])
    return temp


spark = SparkSession \
    .builder \
    .appName("CustomerProfiling") \
    .getOrCreate()

historic_data = spark \
    .read \
    .csv(os.path.curdir + '/original_data/transfer_history.csv', header=True, inferSchema=True)

historic_data.show()

# historic_data.printSchema()

# Convert Value_date to a timestamp

dt1 = F.to_timestamp(F.col("VALUE_DATE"), 'yyyy-MM-dd')
historic_data = historic_data.withColumn("VALUE_DATE_dt", dt1)

# historic_data.printSchema()

# One-Hot Encoder for CURRENCY

customers_1 = historic_data.groupBy('CURRENCY').count().orderBy('count')
# customers_1.show()

grouped_curr = historic_data.groupBy('PARTY_ID') \
    .agg(F.collect_list('CURRENCY').alias('transfer_curr'))
# grouped_curr.show()

# Easier way to do the One hot Encoding compared to the previous method

cv = CountVectorizer(inputCol='transfer_curr', outputCol='TRANSFER_CURRENCY_VEC')

transformed_df = cv.fit(grouped_curr).transform(grouped_curr)
transformed_df.show()

transformed_df.count()
# 2174

sum_A = historic_data.groupBy('PARTY_ID').sum('AMOUNT')
# sum_A.show()

max_A = historic_data.groupBy('PARTY_ID').max('AMOUNT')
# max_A.show()

min_A = historic_data.groupBy('PARTY_ID').min('AMOUNT')
# min_A.show()

avg_A = historic_data.groupBy('PARTY_ID').mean('AMOUNT')
# avg_A.show()

A_df = sum_A.join(avg_A, on=["PARTY_ID"], how="inner")
A_df = A_df.join(min_A, on=["PARTY_ID"], how="inner")
A_df = A_df.join(max_A, on=["PARTY_ID"], how="inner")

# A_df.show()

tranCount = historic_data.groupBy('PARTY_ID').count()
# tranCount.show()

from pyspark.sql.functions import countDistinct

unique_trans = historic_data.groupBy('PARTY_ID').agg(countDistinct('TRANSFER_ID'))
# unique_trans.show()

final_df = transformed_df.join(A_df, on=["PARTY_ID"], how="inner")
# final_df.printSchema()

final_df = final_df.join(tranCount, on=["PARTY_ID"], how="inner")
# final_df.printSchema()

final_df = final_df.join(unique_trans, on=["PARTY_ID"], how="inner")
# final_df.printSchema()

final_df = final_df.withColumnRenamed("count(TRANSFER_ID)", "UNIQ_TRANSFER_ID_COUNT")
final_df.printSchema()

for column in final_df.columns:
    final_df = final_df.withColumnRenamed(column, column.lower())
    if '(' in column:
        temp = column.lower().split('(')
        final_df = final_df.withColumnRenamed(column.lower(), '_'.join([temp[0], temp[1][:-1]]))

final_df.show()

final_df.write.format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="transfer_history", keyspace="customer_profiling")\
    .save()
# final_df.show()

# final_df.toPandas().to_csv('pyspark_transfers.csv')
