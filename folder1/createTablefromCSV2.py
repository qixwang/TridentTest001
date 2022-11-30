import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":

    #Spark session builder
    spark_session = (SparkSession
          .builder
          .appName("Qixiaoapp")
          .config("spark.some.config.option", "some-value")
          .getOrCreate())
    
    spark_context = spark_session.sparkContext
    spark_context.setLogLevel("DEBUG")

    #Import data from csvFilePath to deltaTablePath
    tableName = "yellowtripdata"
    csvFilePath = "abfss://ee2d33da-121f-4c63-bc4a-7ec157f92f29@msit-onelake.pbidedicated.windows.net/8b7e7ef0-619c-4178-8f62-2fe0d221ad8e/Files/csv/NYTaxi_Yellow_22Jan.csv"
    deltaTablePath = "Tables/" + tableName
    deltatablefullpath='abfss://ee2d33da-121f-4c63-bc4a-7ec157f92f29@msit-onelake.pbidedicated.windows.net/8b7e7ef0-619c-4178-8f62-2fe0d221ad8e/Tables/deltaTableSample2'

    df = spark_session.read.format('csv').options(header='true', inferschema='true').load(csvFilePath)
    df.write.mode('overwrite').format('delta').save(deltatablefullpath)