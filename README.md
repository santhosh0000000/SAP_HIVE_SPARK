# SAP_HIVE_SPARK
The code is designed to read data from an SAP HANA database and store it in a Hive table using PySpark.
Spark Session Initialization


def spark_con():
    try:
        spark = (SparkSession.builder
            .appName("SAP_to_Hive")
            ...
            .enableHiveSupport()
            .getOrCreate())
        ...
        return spark
    except Exception as e:
        print(f"Error while creating Spark Session: {e}")
        return None
This function initializes a new Spark session or retrieves the existing one.
.appName("SAP_to_Hive"): Sets the application name for the Spark session.
.config(...): Various configurations like master URL, memory settings, etc., are set.
.enableHiveSupport(): Enables Hive support, allowing PySpark to interact with Hive.
.getOrCreate(): Creates a new Spark session or retrieves an existing one.

Main Function

def main():
    try:
        spark_session = spark_con()
        if spark_session is None:
            print("Spark session could not be created.")
            return
        ...
Reading Data from SAP HANA

        df = (spark_session.read.format("jdbc")
            .option("driver", "com.sap.db.jdbc.Driver")
            ...
            .load())
Checking if DataFrame is Empty

        if df.rdd.isEmpty():
            print("Data frame is empty. Exiting.")
            return

Writing Data to Hive
        df.write.format("orc").option("path", "/demo").mode("overwrite").saveAsTable("default.new20_nestodb01")



