package root

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, WordSpec}

/**
 * User: Constantine Solovev
 * Created: 23.09.17  14:57
 */


trait SparkSpec {

  var sc : SparkContext = _

  def startSpark(sparkConf: SparkConf = new SparkConf()): Unit = {
    sc = SparkContext.getOrCreate(
      sparkConf
        .setMaster("local")
        .setAppName(this.getClass.getSimpleName)
        .set("spark.ui.enabled", "false")
        .set("spark.hadoop.validateOutputSpecs", "false"))
  }

  def stopSpark(): Unit = {
    sc.stop()
  }

}
