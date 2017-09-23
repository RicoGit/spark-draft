package root.aws

import java.io._

import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import root.common.CommandLineJob
import root.domain.Segments

import scala.util.Try

/**
  * User: Constantine Solovev
  * Created: 15.09.17  9:28
  */


object AwsViaHdfsWriterJob extends CommandLineJob {

  private val JobName= "AwsWriterJob"
  private val RootPath = "/tmp/integration"


  override def sparkConf: SparkConf = super.sparkConf
    .setMaster("local[4]")
    .setAppName(JobName)
    .set("spark.ui.enabled", "false")
    .set("hadoop.mapreduce.output.basename", "yourPrefix")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  override protected def run(sc: SparkContext, args: Array[String]): Unit = {

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val ds = spark.read.json("spark-2.2/src/main/resources/segments").as[Segments]

    // one cli agr is one customer
    val customers = Seq("cid1", "cid2")
    val toKey: Segments => String = seg => seg.customerId
    val toLine: Segments => String = seg => s"${seg.userId}\t${seg.segments.mkString(",")}"

    // write to hdfs temp result

    ds
      .filter(seg => customers.contains(seg.customerId))
      .map(seg => toKey(seg) -> toLine(seg))
      .toDF("customerId", "data")
      .repartition($"customerId")
      .sortWithinPartitions("data")
      .write
      .partitionBy("customerId")
      .mode(SaveMode.Overwrite)
      .option("compression", "gzip")
      .text(RootPath)

    val bS3Config = sc.broadcast(S3Config("access", "secret", "bucket", "eu-west-1"))

    // read from hdfs and write to aws in parallel with custom name

    sc.parallelize(customers.map(cid => cid -> getPath(cid)))
        .foreach { case (cid, path) =>
          val s3Config = bS3Config.value

          val fs = getFs // difficult to do this, but possible
          val client = createS3Client(s3Config)

          val files = fs.listFiles(fs.resolvePath(path), false)

          while (files.hasNext) {
            val file = files.next()

            val in: InputStream = fs.open(file.getPath)

            Try {
              val metadata = new ObjectMetadata()
              metadata.setContentLength(file.getLen)
              client.putObject(s3Config.bucket, s"spark-adform-test/cxense/dt=20170809/20170908103301-$cid-user.tsv.gz", in, metadata)
              println(s"$path was success writing")
            } recover { case exception =>
              in.close()
              println(s"WARN: $exception")
            }
          }
        }

  }

  private def getPath(cid: String): Path = new Path(s"$RootPath/customerId=$cid/")

  private def getFs: FileSystem = FileSystem.get(new Configuration())

  private def hdfs2s3(fs: FileSystem, inputFolder: Path, s3Config: S3Config): Unit = {

    val client: AmazonS3 = createS3Client(s3Config)

    val files = fs.listFiles(fs.resolvePath(inputFolder), false)

    while (files.hasNext) {
      val file = files.next()

      val metadata = new ObjectMetadata()
      metadata.setContentLength(file.getLen)

      val in: FSDataInputStream = fs.open(file.getPath)
      client.putObject(s3Config.bucket, "bla-bla-file", in, metadata)
    }

  }

  private def createS3Client(config: S3Config) = {
    val builder = AmazonS3ClientBuilder.standard().withCredentials(new BasicAWSCredentialsProvider(config.accessKey, config.secretKey))
    builder.setRegion(config.region)
    builder.build()
  }
}

case class S3Config(accessKey: String, secretKey: String, bucket: String, region: String)


