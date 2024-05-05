from pyspark.sql import SparkSession
from pyspark.sql import functions as f, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, LongType, TimestampType

# необходимая библиотека с идентификатором в maven
# вы можете использовать ее с помощью метода .config и опции "spark.jars.packages"
spark_jars_packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"

# настройки security для кафки
# вы можете использовать из с помощью метода .options(**kafka_security_options)
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.bootstrap.servers': 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
    'subscribe': 'persist_topic',
}
# определяем схему входного сообщения для JSON
incomming_message_schema = StructType(
    [StructField("subscription_id", IntegerType(), True),
     StructField("name" , StringType(), True),
     StructField("description" , StringType(), True),
     StructField("price", DoubleType(), True),
     StructField("currency" , StringType(), True),
     StructField("key" , StringType(), True),
     StructField("value", StringType(), True),
     StructField("topic" , StringType(), True),
     StructField("partition" , IntegerType(), True),
     StructField("offset", LongType(), True),
     StructField("timestamp" , TimestampType(), True),
     StructField("timestampType" , IntegerType(), True)])

def spark_init() -> SparkSession:
    return SparkSession.builder \
        .appName("persist_topic") \
        .config("spark.jars.packages", spark_jars_packages) \
        .getOrCreate()


def load_df(spark: SparkSession) -> DataFrame:
    return spark.read.format('kafka').options(**kafka_security_options).load()


def transform(df: DataFrame) -> DataFrame:
    return (df.select(f.from_json(f.col("value").cast("string"), incomming_message_schema).alias("parsed_key_value"))
            .select(f.col("parsed_key_value.*")))


spark = spark_init()

source_df = load_df(spark)
df = transform(source_df)


df.printSchema()
df.show(truncate=False)
