package root.hadoop

import java.io.PrintWriter

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.NewHadoopRDD
import root.common.CommandLineJob

/**
 * User: Constantine Solovev
 * Created: 23.09.17  14:21
 */

class SparkWithHadoopApiJob(sc: SparkContext, fs: FileSystem, config: Config) {

  def doWork(): Unit = {
    val rdd = zipLineWithFolderName(config.input)
    val result =
      rdd.map { case (folder, profile) => folder.split("=")(1) -> profile }
         .countByKey()

    println(s" Result is $rdd")

    val out = fs.create(new Path(config.output.concat("/res1")))
    val writer = new PrintWriter(out)
    writer.print(result)
    writer.close()
  }


  def zipLineWithFolderName(rootPath: String) = {
    val paths = fs.listStatus(new Path(rootPath)).map(_.getPath.toString).mkString(",")

   sc.newAPIHadoopFile(paths, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], sc.hadoopConfiguration)
    .asInstanceOf[NewHadoopRDD[LongWritable, Text]]
    .mapPartitionsWithInputSplit { case (inputSplit, lineIterator) =>
      val fileSplit = inputSplit.asInstanceOf[FileSplit]
      val folder = inputSplit.asInstanceOf[FileSplit].getPath.getParent.getName
      lineIterator.map { case (_, line) => folder -> line.toString }
    }
  }

}

object SparkWithHadoopApiJob extends CommandLineJob {
  /**
   * Primary method. Override this method and put all logic here
   */
  override protected def run(sc: SparkContext, args: Array[String]): Unit = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    SparkWithHadoopApiJob(sc, fs, Config.fromArgs(args)).doWork()
  }

  def apply(sc: SparkContext, fs: FileSystem, config: Config): SparkWithHadoopApiJob = {
    new SparkWithHadoopApiJob(sc, fs, config)
  }

}

case class Config(input: String, output: String)

object Config {
  def fromArgs(args: Array[String]): Config = Config(args(0), args(1))
}
