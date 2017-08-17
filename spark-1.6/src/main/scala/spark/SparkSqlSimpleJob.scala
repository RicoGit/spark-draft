package spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * User: Constantine Solovev
  * Created: 26.01.16  9:08
  */


object SparkSqlSimpleJob {

  def main (args: Array[String]) {

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.getSimpleName)
      .set("spark.ui.enabled", "false")
      .set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(conf)


    val sampleFile: String = getClass.getResource("/spark/sparkSqlSample.json").getPath

    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.json(sampleFile)

    df.registerTempTable("people")
    df.printSchema()

    val select = sqlContext.sql("select * from people where value = 1280474.0").collectAsList()

    val names = df.select("name").collectAsList()

    println(names)
    println(select)

//    sc.makeRDD(Seq("" -> 2)).join(sc.emptyRDD)

  }

}
