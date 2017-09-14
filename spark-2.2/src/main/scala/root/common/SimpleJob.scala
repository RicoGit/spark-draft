package root.common

import com.typesafe.scalalogging.slf4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/** Base parent for all spark spark.job. */
trait SimpleJob {

  protected val logger = Logger(LoggerFactory.getLogger(this.getClass.getName))

  def main(args: Array[String]) = {
    logger.info("Job starts")
    val sc: SparkContext = createSparkContext
    try {
      doWork(sc)
    } finally {
      sc.stop()
    }
  }

  /**
   * Primary method. Override this method and put all logic here
   */
  protected def doWork(sc: SparkContext)

  protected def sparkConf = new SparkConf()

  protected def createSparkContext = new SparkContext(sparkConf)

}
