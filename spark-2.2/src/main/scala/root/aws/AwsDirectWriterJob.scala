package root.aws

import java.io._

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ObjectMetadata
import org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import root.common.SimpleJob
import root.domain.Segments

import scala.collection.mutable

/**
  * User: Constantine Solovev
  * Created: 15.09.17  9:28
  */


object AwsDirectWriterJob extends SimpleJob {

  private val JobName= "AwsWriterJob"

  override def sparkConf: SparkConf = super.sparkConf
    .setMaster("local[2]")
    .setAppName(JobName)
    .set("spark.ui.enabled", "false")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  override protected def doWork(sc: SparkContext): Unit = {

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val ds = spark.read.json("spark-2.2/src/main/resources/segments").as[Segments]

    ds.show(3)

    val toKey: Segments => String = seg => seg.customerId
    val toLine: Segments => String = seg => s"${seg.userId}\t${seg.segments.mkString(",")}"

    ds.foreachPartition(writeLinesToLocalFs(_, toKey, toLine))

  }

  def writeLinesToLocalFs[V](iterator: Iterator[V], toKey: V => String, toLine: V => String): Unit = {

    val writers = new mutable.HashMap[String, Writer]
    try {
      while (iterator.hasNext) {

        iterator.next() match {
          case segment =>
            val key = toKey(segment)
            val writer = writers.get(key) match {
              case Some(wr) => wr
              case None =>

                // aws

                val accessKey = "key"
                val secretKey = "secret"
                val bucketName = "backet"
                val prefix = "prefix"

                val client = AmazonS3ClientBuilder.standard()
                  .withCredentials(new BasicAWSCredentialsProvider(accessKey, secretKey))
                  .build()

                /**
                  * Note that! This approach isn't work because aws required content length, we should fetch all data from
                  * iterator and get length. It's ugly and badly applicable to spark, we will catch OOM with big data of partition
                  */

                val in = new PipedInputStream

                client.putObject(bucketName, prefix, in, new ObjectMetadata())

                val newWriter = new OutputStreamWriter(new BufferedOutputStream(new PipedOutputStream(in)))
                writers.put(key, newWriter)
                newWriter
            }

            writer.write(toLine(segment))
          case it => throw new IllegalStateException(s"$it is wrong record!")
        }
      }
    } finally {
      writers.values.foreach(_.flush())
      writers.values.foreach(_.close())
    }
  }

}

