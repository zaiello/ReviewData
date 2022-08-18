import pyspark
from pyspark.sql import SparkSession, Row
import pprint
import json
from pyspark.sql.types import StructType, FloatType, LongType, StringType, StructField
import pyspark.sql.functions
from pyspark.sql.functions import struct, col, when, count

#### Set up ####

def To_numb(x):
  x['asin'] = str(x['asin'])
  x['overall'] = int(x['overall'])
  x['reviewText'] = str(x['reviewText'])
  x['unixReviewTime'] = int(x['unixReviewTime'])
  x['reviewerID'] = str(x['reviewerID'])
  return x

sc = pyspark.SparkContext()

# PACKAGE_EXTENSIONS= ('gs://hadoop-lib/bigquery/bigquery-connector-hadoop2-latest.jar')

bucket = sc._jsc.hadoopConfiguration().get('fs.gs.system.bucket')
project = sc._jsc.hadoopConfiguration().get('fs.gs.project.id')
input_directory = 'gs://{}/hadoop/tmp/bigquerry/pyspark_input'.format(bucket)
output_directory = 'gs://{}/pyspark_demo_output'.format(bucket)

spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('flights') \
  .getOrCreate()

conf={
    'mapred.bq.project.id':project,
    'mapred.bq.gcs.bucket':bucket,
    'mapred.bq.temp.gcs.path':input_directory,
    'mapred.bq.input.project.id': "cs-511-p1-337622",
    'mapred.bq.input.dataset.id': 'final_project',
    'mapred.bq.input.table.id': 'final_table',
}

## pull table from big query
table_data = sc.newAPIHadoopRDD(
    'com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat',
    'org.apache.hadoop.io.LongWritable',
    'com.google.gson.JsonObject',
    conf = conf)

## convert table to a json like object, make sure all the columns are in the correct format
vals = table_data.values()
vals = vals.map(lambda line: json.loads(line))
vals = vals.map(To_numb)

## schema 
schema = StructType([
   StructField("asin", StringType(), True),
   StructField("overall", LongType(), True),
   StructField("reviewText", StringType(), True),
   StructField("unixReviewTime", LongType(), True),
   StructField("reviewerID", StringType(), True)])

## create a dataframe object
df = spark.createDataFrame(vals, schema= schema)


df.repartition(6)

#### Question 1 ####

# create view with df so we can use sql to select stuff
df.createOrReplaceTempView('review')

# Get min and max review times for each asin
min_time_df = spark.sql("Select asin, MIN(unixReviewTime) as min_time FROM review GROUP BY asin")
max_time_df = spark.sql("Select asin, MAX(unixReviewTime) as max_time FROM review GROUP BY asin")

# Join df with the min review time by asin
df = df\
    .join(min_time_df, "asin")\

# Join df with the max review time by asin
# Add column: Product listing lifetime in months
# difference between the min and max unix timestamp divided by 30 days = 2592000 seconds
df = df\
    .join(max_time_df, "asin")\
    .withColumn('list_life', ((col("max_time") - col("min_time"))/2592000))\

# number of reviews in the product's lifetime
review_count_df = spark.sql("Select asin, count(distinct reviewText) as review_count FROM review GROUP BY asin")

# join df with the review_count_df
df = df\
    .join(review_count_df, "asin")

# update view
df.createOrReplaceTempView('review')

# https://sparkbyexamples.com/pyspark/pyspark-add-new-column-to-dataframe/
# https://sparkbyexamples.com/spark/spark-case-when-otherwise-example/

# first 6 months
# find the review count between the first review time and the time 6 months later (6 months = 15770000 seconds)
six_mo_df = spark.sql("Select asin, count(distinct reviewText) as six_mo FROM review WHERE unixReviewTime BETWEEN (min_time) AND (min_time+15770000) GROUP BY asin")

# review count over the second 6 months
sec_six_df = spark.sql("Select asin, count(distinct reviewText) as sec_six FROM review WHERE unixReviewTime BETWEEN (min_time+15770000) AND (min_time+31540000) GROUP BY asin")

# review count between the first and second year
sec_yr_df = spark.sql("Select asin, count(distinct reviewText) as sec_yr FROM review WHERE unixReviewTime BETWEEN (min_time+31540000) AND (min_time+63080000) GROUP BY asin")

# # review count between the second and fifth year
five_yr_df = spark.sql("Select asin, count(distinct reviewText) as five_yr FROM review WHERE unixReviewTime BETWEEN (min_time+63080000) AND (min_time+157700000) GROUP BY asin")

# add all the columns to the df
df = df\
    .join(six_mo_df, "asin")

df = df\
    .join(sec_six_df, "asin")

df = df\
    .join(sec_yr_df, "asin")

df = df\
    .join(five_yr_df, "asin")

# update the view
df.createOrReplaceTempView('review')

# get the important columns
final_answer = spark.sql('Select distinct asin, six_mo, sec_six, sec_yr, five_yr, review_count FROM review')

# show the answer
final_answer.show()

# Save the data to BigQuery
# https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example#pyspark
# https://stackoverflow.com/questions/62605415/pyspark-sql-utils-illegalargumentexception-requirement-failed-temporary-gcs-pa
# IMPORTANT: 
# in the jar files when running the job add gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar 

# df.write.format('bigquery') \
  # .option('temporaryGcsBucket', 'cs512_bigquery') \
  # .save('final_project.edited_dataset')

## deletes the temporary files
input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True)


