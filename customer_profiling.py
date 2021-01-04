from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator, TrainValidationSplit

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import split, udf
from pyspark.sql.types import StringType, IntegerType, FloatType

spark = SparkSession.builder.appName('CustomerProfiling').getOrCreate()
spark.sparkContext.setLogLevel('INFO')

# read messages from Kafka
streaming_data = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "customer-profiling") \
    .option('startingOffsets', 'earliest') \
    .load()

streaming_data = streaming_data.selectExpr('CAST(value as STRING)')

customer_login = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="customer_login", keyspace="customer_profiling") \
    .load()

customer_login.show()

invoices = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="invoices", keyspace="customer_profiling") \
    .load()

invoices.show()

transfers = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="transfer_history", keyspace="customer_profiling") \
    .load()
transfers.show()

static_data = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="static_data", keyspace="customer_profiling") \
    .load()
static_data.show()

new_df = invoices.withColumnRenamed('count', 'invoices_count')\
                 .join(transfers.withColumnRenamed('count', 'transfers_count'), on=['party_id'], how='inner')\
                 .join(static_data, on=['party_id'], how='inner')
new_df.show()

# drop string features
train_df = new_df.drop('curr', 'currency_vec', 'transfer_curr', 'transfer_currency_vec', 'features')

# train_df = train_df.rdd.map(lambda row: (Vectors.dense(row),)).toDF(['features'])
# train_df.printSchema()

assembler = VectorAssembler().setInputCols(train_df.columns).setOutputCol('features').setHandleInvalid('skip')
train_vector = assembler.transform(train_df)

kmeans = KMeans(k=4).setFeaturesCol('features')  # 4 clusters here
# model = kmeans.fit(train_vector)
evaluator = ClusteringEvaluator()

paramGrid = ParamGridBuilder().build()

train_validation_split = TrainValidationSplit() \
    .setEstimator(Pipeline(stages=[kmeans])) \
    .setEvaluator(evaluator) \
    .setEstimatorParamMaps(paramGrid)

model = train_validation_split.fit(train_vector)
transformed = model.transform(train_vector)
# cross_validator.getEvaluator().evaluate(transformed)
transformed.printSchema()

# pred_count = transformed.groupBy('prediction').count().orderBy('count')

streaming_data.writeStream.format('console').trigger(continuous='1 second').start()

split_columns = split(streaming_data['value'], ',')
strip_dq = udf(lambda x: x.replace('[', '').replace('"', '').replace(']', ''), StringType())


def get_type(col):
    int_cols = {'party_id', 'count', 'county', 'index', 'uniq_transfer_id_count', 'lob_code', 'package', 'region',
                'size'}
    string_cols = {'features', 'curr', 'currency_vec', 'transfer_cur', 'transfer_currency_vec'}
    if col in int_cols:
        return IntegerType()
    elif col in string_cols:
        return None
    else:
        return FloatType()


# convert streaming df to multicolumnar df
columns = ['id', 'PARTY_ID', 'curr', 'CURRENCY_VEC', 'sum(TAXINCLUSIVEAMOUNT)', 'avg(TAXINCLUSIVEAMOUNT)',
           'min(TAXINCLUSIVEAMOUNT)', 'max(TAXINCLUSIVEAMOUNT)', 'sum(TAXEXCLUSIVEAMOUNT)', 'avg(TAXEXCLUSIVEAMOUNT)',
           'min(TAXEXCLUSIVEAMOUNT)', 'max(TAXEXCLUSIVEAMOUNT)', 'count_x', 'min(INV_PERIOD)', 'max(INV_PERIOD)',
           'avg(INV_PERIOD)', 'transfer_curr', 'TRANSFER_CURRENCY_VEC', 'sum(AMOUNT)', 'avg(AMOUNT)', 'min(AMOUNT)',
           'max(AMOUNT)', 'county', 'UNIQ_TRANSFER_ID_COUNT', 'LOB_CODE', 'SIZE', 'PACKAGE', 'REGION', 'COUNTY']

for i, val in enumerate(columns):
    if get_type(val.lower()) is None or val == 'id':
        continue

    if '(' in val:
        temp = val.lower().split('(')
        val = '_'.join([temp[0], temp[1][:-1]])
    streaming_data = streaming_data.withColumn(val.lower(), split_columns.getItem(i))
    streaming_data = streaming_data.withColumn(val.lower(), streaming_data[val].cast(get_type(val)))
streaming_data = streaming_data.drop('value')

assembler = VectorAssembler().setInputCols(streaming_data.columns).setOutputCol('features').setHandleInvalid('skip')
vector = assembler.transform(streaming_data)


def write_to_cassandra(df: DataFrame, batch_id: int):
    df.persist()
    df \
        .write\
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="predictions", keyspace="customer_profiling") \
        .save()
    # df \
    #     .selectExpr('CAST(features as STRING)') \
    #     .show()
    df.unpersist()


model.transform(vector) \
    .select('features', 'prediction') \
    .writeStream \
    .trigger(processingTime='5 seconds') \
    .foreachBatch(write_to_cassandra) \
    .start() \
    .awaitTermination()
