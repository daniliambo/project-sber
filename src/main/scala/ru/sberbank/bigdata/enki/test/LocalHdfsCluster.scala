package ru.sberbank.bigdata.enki.test

import java.nio.file.{Files, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.CommonConfigurationKeysPublic
import org.apache.hadoop.hdfs.MiniDFSCluster.Builder
import org.apache.hadoop.hdfs.{DistributedFileSystem, MiniDFSCluster}
import org.apache.log4j.{Level, Logger}
import ru.sberbank.bigdata.enki.test.TestConstants._

import scala.reflect.io.Path._

private[enki] object LocalHdfsCluster {
  lazy val hadoopWarehouseDir: Path = Files.createTempDirectory("hadoop-warehouse")
  lazy val hadoopTmpDir: Path       = Files.createTempDirectory("hadoop-tmp-warehouse")

  val conf = new Configuration(true)
  conf.set("hadoop.tmp.dir", hadoopTmpDir.toAbsolutePath.toString)
  // avoid url parse exception
  conf.set("dfs.datanode.data.dir", (hadoopTmpDir.toFile / "dfs" / "data").toURI.toString)

  // TODO Check conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, ???)

  lazy val cluster: MiniDFSCluster = {
    Logger.getLogger("BlockStateChange").setLevel(Level.ERROR)
    Logger.getLogger("DataNucleus").setLevel(Level.ERROR)
    Logger.getLogger("hive.log").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.ERROR)

    // hack to choose directory for hadoop test
    System.setProperty("user.dir", hadoopWarehouseDir.toAbsolutePath.toString)

    new Builder(conf)
      .nameNodePort(NameNodePort)
      // true will not work with hack above
      .manageDataDfsDirs(false)
      .build()
  }

  val clusterFS: DistributedFileSystem = cluster.getFileSystem

  private val clusterConf = cluster.getConfiguration(0)

  val getDefaultFS: String = clusterConf.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY)

}
