package root.simple

import java.io.{BufferedOutputStream, OutputStreamWriter, Writer}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable


/**
 * User: Constantine Solovev
 * Created: 14.09.17  21:02
 */


object WriteToMultiFiles {

  case class Record(uid: String, sids: Seq[String])

  def main(args: Array[String]) {

    val spark =
      SparkSession.builder
        .appName("Simple Application")
        .master("local[2]")
        .config(new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))
        .getOrCreate()

    import spark.implicits._

    val ds = spark.createDataset(
      Seq("cid1" -> Record("uid1", Seq("s1", "s2", "s3")), "cid2" -> Record("uid2", Seq("s2", "s3", "s4")))
    )

    ds.repartition(4).foreachPartition(partition => writeLinesToLocalFs(partition, toLine))

  }

  private def writeLinesToLocalFs[V](iterator: Iterator[(String, V)], toLine: V => String): Unit = {

    val writers = new mutable.HashMap[String, Writer]
    try {
      while (iterator.hasNext) {

        iterator.next() match {
          case (key, rec) =>
            val writer = writers.get(key) match {
              case Some(wr) => wr
              case None =>
                val newWriter = new OutputStreamWriter(
                  new BufferedOutputStream(
                    FileSystem.getLocal(new Configuration()).create(new Path("/tmp/" + key + ".txt"), true)
                  )
                )
                writers.put(key, newWriter)
                newWriter
            }

            writer.write(toLine(rec))
          case it => throw new IllegalStateException(s"$it is wrong record!")
        }
      }
    } finally {
      writers.values.foreach(_.flush())
      writers.values.foreach(_.close())
    }
  }

  private def toLine(record: Record): String = s"${record.uid}\t${record.sids.mkString(",")}\n"

}


