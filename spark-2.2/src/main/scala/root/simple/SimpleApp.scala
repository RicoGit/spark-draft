package root.simple

import org.apache.spark.sql.{Dataset, SparkSession}


/* SimpleApp.scala */


object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "spark-2.2/src/main/resources/file.txt"
    val spark = SparkSession.builder.appName("Simple Application").master("local").getOrCreate()
    val logData: Dataset[String] = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}
