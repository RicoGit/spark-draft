package root.simple

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


/* SimpleApp.scala */


object DataFrameApp {
  def main(args: Array[String]) {

    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val profilesDf: DataFrame = spark.read.json("spark-2.2/src/main/resources/profiles").cache()

    val x = profilesDf.groupBy($"system").agg(concat($"id"))
    x.show()
    x.printSchema()

    spark.stop()
  }
}
