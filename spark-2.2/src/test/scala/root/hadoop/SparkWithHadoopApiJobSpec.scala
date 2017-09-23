package root.hadoop

import java.io.File

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import org.scalatest.{BeforeAndAfter, Matchers, WordSpecLike}
import root.SparkSpec

/**
 * User: Constantine Solovev
 * Created: 23.09.17  14:51
 */


class SparkWithHadoopApiJobSpec extends SparkSpec with WordSpecLike with Matchers with BeforeAndAfter {

  var cluster: MiniDFSCluster = _
  var fs: FileSystem = _

  before {
    System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA)

    val conf = new HdfsConfiguration
    val testDataCluster = new File(System.getenv("java.io.tmpdir"), "cluster")
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, testDataCluster.getAbsolutePath)
    cluster = new MiniDFSCluster.Builder(conf).build

    fs = FileSystem.get(conf)

    startSpark()

    sc.hadoopConfiguration.addResource(conf) // important: adds hdfs config for spark
  }

  after {
    if (cluster != null) {
      cluster.shutdown
      cluster = null
    }
    stopSpark()
  }

  "job" should {
    "success" in {

      val root = fs.getWorkingDirectory

      // copy from test resources 3 files
      copyFileFromResourcesToHdfs(fs, "/profiles/cid1", "data/profiles/type=raw/part-00000")
      copyFileFromResourcesToHdfs(fs, "/profiles/cid2", "data/profiles/type=mapped/part-00000")
      copyFileFromResourcesToHdfs(fs, "/profiles/cid3", "data/profiles/type=other/part-00000")

      val config = Config("data/profiles", "data/results")
      SparkWithHadoopApiJob(sc, fs, config).doWork()

      val result = sc.textFile(config.output.concat("/res1")).take(1)(0)
      result shouldBe "Map(mapped -> 10, other -> 10, raw -> 8)"
    }
  }

  def copyFileFromResourcesToHdfs(fs: FileSystem, resourceName: String, destinationPath: String): Unit = {
    val resource = getClass.getResource(resourceName)
    fs.copyFromLocalFile(new Path(resource.getPath), new Path(destinationPath) )
  }

}
