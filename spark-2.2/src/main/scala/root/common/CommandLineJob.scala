package root.common

import com.typesafe.scalalogging.slf4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/** Base parent for all spark spark.job. */
trait CommandLineJob {

  protected val logger = Logger(LoggerFactory.getLogger(this.getClass.getName))

  def main(args: Array[String]) = {
    logger.info(s"Job start with arguments: ${args.mkString(",")}")
    val sc: SparkContext = createSparkContext
    try {
      run(sc, args)
    } finally {
      sc.stop()
    }
  }

  /**
   * Primary method. Override this method and put all logic here
   */
  protected def run(sc: SparkContext, args: Array[String])

  protected def sparkConf = new SparkConf()

  protected def createSparkContext = new SparkContext(sparkConf)

}
