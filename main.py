import os
from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("CSV to MySQL") \
        .config("spark.jars", "payh\\mysql-connector-j-8.0.32.jar") \
        .getOrCreate()

mysql_props = {
    "user": "user",
    "password": "password",
    "driver": "com.mysql.jdbc.Driver",
    "characterEncoding": "UTF-8"
}

csv_folder = "path_folder_csv"
csv_files = [os.path.join(csv_folder, f) for f in os.listdir(csv_folder) if f.endswith('.csv')]

for csv_file in csv_files:
    df = spark.read \
        .option("header", "True") \
        .option("sep", ";") \
        .option("encoding", "cp1251") \
        .option("inferSchema", "true") \
        .csv(csv_file)
# hello
    df.write \
        .option("batchsize", "10000") \
        .option("numPartitions", "4") \
        .jdbc(url="jdbc:mysql://host:port/db?characterEncoding=UTF-8&rewriteBatchedStatements=true",
              table="pers_mass", mode="append", properties=mysql_props)
