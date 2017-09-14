package root.simple

import org.apache.spark.sql.{DataFrame, SparkSession}


/* SimpleApp.scala */


object ParquetApp {

  private val logFile = "spark-2.2/src/main/resources/profiles"

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder
        .appName("Simple Application")
        .master("local")
        .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    // read json as DataFrame
    val df: DataFrame = spark.read.json(logFile).cache()
    // show schema
    df.printSchema()
    // select col id and show data
    df.select($"id").show(1)
    // create table view and make sql queries
    df.createOrReplaceTempView("profiles")
    val result: DataFrame = spark.sql("SELECT * FROM profiles WHERE system <> null")
    result.show()

    spark.stop()
  }

}

