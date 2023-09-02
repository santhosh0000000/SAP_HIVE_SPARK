from pyspark.sql import SparkSession

def spark_con():
    try:
        spark = (SparkSession.builder
                 .appName("SAP_to_Hive")
                 .config("spark.master", "local[*]")
                 .config("spark.driver.memory", "32g")
                 .config("spark.executor.memory", "3g")
                 .config("spark.executor.instances", "12")
                 .config("spark.executor.cores", "3")
                 .config("spark.jars", "/root/ngdbc-2.17.12.jar")
                 .config("hive.metastore.uris", "thrift://hdp01-preprod.geepas.local:9083")
                 .config("hive.metastore.client.capability.check", "false")
                 .enableHiveSupport()
                 .getOrCreate())

        spark.conf.set("spark.sql.shuffle.partitions", 4)
        spark.conf.set("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")
        return spark
    except Exception as e:
        print(f"Error while creating Spark Session: {e}")
        return None

def main():
    try:
        spark_session = spark_con()
        if spark_session is None:
            print("Spark session could not be created.")
            return

        df = (spark_session.read.format("jdbc")
              .option("driver", "com.sap.db.jdbc.Driver")
              .option("url", "jdbc:sap://POrd.MRD.local:30215/SAPTER")
              .option("user", "BIG_DATA_USER")
              .option("password", "Geepas123")
              .option("dbtable", "SAPTER.VBTAK")
              .option("fetchSize", "1000")
              .option("numPartitions", 200)
              .load())

        # Check if DataFrame is empty
        if df.rdd.isEmpty():
            print("Data frame is empty. Exiting.")
            return

        try:
            # Writing DataFrame to Hive table in ORC format
           df.write.format("orc").option("path", "/demo").mode("overwrite").saveAsTable("default.new20_nestodb01")
        except Exception as e:
            print(f"Error while writing to Hive: {e}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
