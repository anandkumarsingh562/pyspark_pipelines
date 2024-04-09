from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType,LongType,DoubleType,BooleanType 
from pyspark.sql.functions import col, row_number, monotonically_increasing_id,desc,when,count,lit ,concat, regexp_replace , to_date , current_timestamp , rand

spark=SparkSession.builder.master("local[*]").config("spark.driver.extraClassPath", "C:/documents/oracle/jdbc/driver.jar").config("spark.jars", "/path/to/snowflake-jdbc.jar,/path/to/spark-snowflake_2.12.jar").getOrCreate()

"""extracting_data_from_oracle_in_configs_actual_credentials_are_anonymized"""


oracle_properties = {
    "user": "your_username",
    "password": "your_password",
    "driver": "oracle.jdbc.driver.OracleDriver",
    "url": "jdbc:oracle:thin:@//hostname:port/service_name"
}

Banking_data = spark.read.jdbc(url=oracle_properties["url"],table="Bank_clients",properties=oracle_properties)


"""cleaning and transforming of data to enhance analysis and reporting"""

Banking_data = Banking_data.withColumn("job",col("job").cast("string"))


Banking_data = Banking_data.withColumn("job",regexp_replace("job","\\.","_"))


Banking_data = Banking_data.withColumn("education",regexp_replace(col("education"),"\\.","_"))

Banking_data = Banking_data.withColumn("education",when(col("education")=="unknown",None).otherwise(col("education")))


Banking_data = Banking_data.withColumn("credit_default",col("credit_default").cast("Boolean"))

Banking_data = Banking_data.withColumn("mortgage",col("mortgage").cast("Boolean"))

Banking_data.select("credit_default","mortgage").show()

Banking_data = Banking_data.withColumn("previous_outcome",col("previous_outcome").cast("Boolean"))

Banking_data = Banking_data.withColumn("campaign_outcome",col("campaign_outcome").cast("Boolean"))

Banking_data = Banking_data.withColumn("month",when(col("month")=="jan","01").when(col("month")=="feb","02").when(col("month")=="mar","03").when(col("month")=="apr","04").when(col("month")=="may","05").when(col("month")=="jun","06").when(col("month")=="jul","07").when(col("month")=="aug","08").when(col("month")=="sep","09").when(col("month")=="oct","10").when(col("month")=="nov","11").when(col("month")=="dec","12").otherwise(None))

Banking_data.select("previous_outcome","campaign_outcome").show()

Banking_data = Banking_data.withColumn("last_contact_date",concat(lit("2022-"),col("month"),lit("-"),col("day")))

Banking_data.select("last_contact_date").distinct().show()

Banking_data = Banking_data.withColumn("last_contact_date",to_date("last_contact_date"))


client_df = Banking_data.select("client_id","age","job","martial_status","education","credit_default","mortgage")

campaign_df = Banking_data.select("client_id","number_contacts","contact_duration","previous_campaign_contacts","previous_outcome","campaign_outcome","last_contact_date")

client_df.write.csv("C:/Users/xavier_dp/Downloads/data"+"/"+"client3.csv",header=True)

campaign_df.write.csv("C:/Users/xavier_dp/Downloads/data"+"/"+"campaign3.csv",header=True)

economics_df = Banking_data.select("client_id","cons_price_idx","euribor_three_months")

economics_df.write.csv("C:/Users/xavier_dp/Downloads/data"+"/"+"econumics1.csv",header=True)


"""addition of columns like etl_create_ts and batch_no to enhance analysis and reporting"""
client_df = client_df.withColumn("etl_create_ts",current_timestamp())

client_df = client_df.withColumn("batch_no",rand()*100000)

"""loading to snowflake"""

sfOptions = {
  "URL" : "account.snowflakecomputing.com",
  "Database" : "database",
  "Warehouse" : "warehouse",
  "Schema" : "schema",
  "User" : "username",
  "Password" : "password",
}


client_df.write.format("snowflake").options(**sfOptions).option("dbtable", "Bank_clients").mode("overwrite").save()


spark.stop()
