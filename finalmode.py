from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, LongType

# Main function to run Spark session and SQL queries
if __name__ == "__main__":
        # Initialize the Spark session
        spark = (SparkSession.builder
                .appName("ModeFrequencyDemo")
                #.master("local")
                #.config("spark.jars", "target/scala-2.12/mode_2.12-0.1.jar") \
                .getOrCreate())

        print("Mode Frequency")

        # Sample data
        data = [("cat",), ("dog",), ("lion",), ("wolf",), ("cat",),
                ("dog",), ("tiger",), ("lion",), ("wolf",), ("cat",),
                ("tiger",)]

        # Create a DataFrame from the sample data
        df = spark.createDataFrame(data, ["value"])
        df.createOrReplaceTempView("my_table")

        # Show the DataFrame
        df.show()

        # Register the Scala UDAF
        spark.sql("CREATE FUNCTION mode_function AS 'MostFrequentValueUDAF'")
        


        # Use the registered UDAF in a SQL query
        spark.sql("SELECT mode_function(value) AS mode_result FROM my_table").show()

        # Stop Spark session
        spark.stop()



