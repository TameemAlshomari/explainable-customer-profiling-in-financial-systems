import os

import pyspark.sql.functions as F
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.linalg import Vectors
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession


def convert_value_to_row(df: DataFrame):
    temp = df.rdd.map(lambda x: Vectors.dense(x.split(',')[:-1])).toDF(['features'])
    return temp


spark = SparkSession \
    .builder \
    .appName("CustomerProfiling") \
    .getOrCreate()
historic_data = spark \
    .read \
    .csv(os.path.curdir + '/original_data/invoice_history.csv', header=True, inferSchema=True)
historic_data.summary().show()
historic_data.printSchema()

# dt1 = historic_data.select(F.to_timestamp(historic_data.DUEDATE, 'yyyy-MM-dd').alias('DUEDATE_dt'))
# dt2 = historic_data.select(F.to_timestamp(historic_data.ISSUEDATE, 'yyyy-MM-dd').alias('ISSUEDATE_dt'))
dt1 = F.to_timestamp(F.col("DUEDATE"), 'yyyy-MM-dd')
historic_data = historic_data.withColumn("DUEDATE_dt", dt1)

historic_data.printSchema()

# historic_data.select(F.col("DUEDATE_dt")).show()
dt2 = F.to_timestamp(F.col("ISSUEDATE"), 'yyyy-MM-dd')

# dt2
historic_data = historic_data.withColumn("ISSUEDATE_dt", dt2)
# historic_data.printSchema()

timeDiff = (F.unix_timestamp(F.col('DUEDATE_dt'), "yyyy-MM-dd HH:mm:ss") - F.unix_timestamp(F.col('ISSUEDATE_dt'),
                                                                                            "yyyy-MM-dd HH:mm:ss"))

# return the difference in days
historic_data = historic_data.withColumn("INV_PERIOD", timeDiff / (24 * 3600))
# historic_data.show()
historic_data.select(F.col("INV_PERIOD")).show()

# One-Hot Encoder for CURRENCY
# customers_1 = historic_data.groupBy('CURRENCY').count().orderBy('count')
# customers_1.show()
from pyspark.ml.feature import StringIndexer

indexer = StringIndexer(inputCol="CURRENCY", outputCol="CURRENCY_INDEX")
indexed = indexer.fit(historic_data).transform(historic_data)
# indexed.show()
from pyspark.ml.feature import OneHotEncoder

encoder = OneHotEncoder(inputCols=["CURRENCY_INDEX"],
                        outputCols=["CURRENCY_ENCODED"], dropLast=False)
model = encoder.fit(indexed)
encoded = model.transform(indexed)
# encoded.show()
encoded.select(F.col("CURRENCY_ENCODED")).where(F.col("CURRENCY") == "SEK").show()
encoded = encoded.drop("CURRENCY_ENCODED")

# Easier way to do the OnehotEncoding compared to the previous method
grouped_curr = encoded.groupBy('PARTY_ID') \
    .agg(F.collect_list('CURRENCY').alias('curr'))

cv = CountVectorizer(inputCol='curr', outputCol='CURRENCY_VEC')

transformed_df = cv.fit(grouped_curr).transform(grouped_curr)
# transformed_df.show()
# transformed_df.count()
# TAXINCUSIVE AMOUNT aggregated features
sum_TIA = historic_data.groupBy('PARTY_ID').sum('TAXINCLUSIVEAMOUNT')
# sum_TIA.show()
max_TIA = historic_data.groupBy('PARTY_ID').max('TAXINCLUSIVEAMOUNT')
# max_TIA.show()
min_TIA = historic_data.groupBy('PARTY_ID').min('TAXINCLUSIVEAMOUNT')
# min_TIA.show()
avg_TIA = historic_data.groupBy('PARTY_ID').mean('TAXINCLUSIVEAMOUNT')
# avg_TIA.show()

TIA_df = sum_TIA.join(avg_TIA, on=["PARTY_ID"], how="inner")
TIA_df = TIA_df.drop("max(TAXINCLUSIVEAMOUNT)")
# TIA_df.printSchema()

TIA_df = TIA_df.join(min_TIA, on=["PARTY_ID"], how="inner")
TIA_df = TIA_df.join(max_TIA, on=["PARTY_ID"], how="inner")
# TIA_df.show()
TIA_df.count()
# TAXEXCUSIVE AMOUNT aggregated features
sum_TEA = historic_data.groupBy('PARTY_ID').sum('TAXEXCLUSIVEAMOUNT')
min_TEA = historic_data.groupBy('PARTY_ID').min('TAXEXCLUSIVEAMOUNT')
max_TEA = historic_data.groupBy('PARTY_ID').max('TAXEXCLUSIVEAMOUNT')
avg_TEA = historic_data.groupBy('PARTY_ID').mean('TAXEXCLUSIVEAMOUNT')
# sum_TEA.show()
TEA_df = sum_TEA.join(avg_TEA, on=["PARTY_ID"], how="inner")
TEA_df = TEA_df.join(min_TEA, on=["PARTY_ID"], how="inner")
TEA_df = TEA_df.join(max_TEA, on=["PARTY_ID"], how="inner")
# TEA_df.show()
# Create INV_COUNT feature
invCount = historic_data.groupBy('PARTY_ID').count().orderBy('count')
# invCount.show()

# INV_PERIOD aggregated features
# inv_count = customers_1.select(F.col("count")).where(F.col('count') > 200)
# inv_count.show()

# sum_period = historic_data.groupBy('PARTY_ID').sum('TAXEXCLUSIVEAMOUNT')
min_period = historic_data.groupBy('PARTY_ID').min('INV_PERIOD')
max_period = historic_data.groupBy('PARTY_ID').max('INV_PERIOD')
avg_period = historic_data.groupBy('PARTY_ID').mean('INV_PERIOD')

# min_period.show()
period_df = min_period.join(max_period, on=["PARTY_ID"], how="inner")
period_df = period_df.join(avg_period, on=["PARTY_ID"], how="inner")
# period_df.show()
period_df.count()
# Merging all transformations in one table
final_df = transformed_df.join(TIA_df, on=["PARTY_ID"], how="inner")
final_df.printSchema()
final_df = final_df.join(TEA_df, on=["PARTY_ID"], how="inner")
final_df.printSchema()
final_df = final_df.join(invCount, on=["PARTY_ID"], how="inner")
final_df.printSchema()
final_df = final_df.join(period_df, on=["PARTY_ID"], how="inner")
final_df.printSchema()
# final_df.show()

for column in final_df.columns:
    final_df = final_df.withColumnRenamed(column, column.lower())
    if '(' in column:
        temp = column.lower().split('(')
        final_df = final_df.withColumnRenamed(column.lower(), '_'.join([temp[0], temp[1][:-1]]))
final_df.show()

final_df.write.format("org.apache.spark.sql.cassandra") \
    .mode('append') \
    .options(table="invoices", keyspace="customer_profiling") \
    .save()
