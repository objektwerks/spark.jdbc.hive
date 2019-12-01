package spark

import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

object SparkInstance {
  private val logger = Logger.getLogger(getClass.getSimpleName)

  val sparkWarehouseDir = new File("./target/spark-warehouse").getAbsolutePath
  val sparkEventLogDir = "/tmp/spark-events"
  makeSparkEventLogDir(sparkEventLogDir)

  val sparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("jdbc-hive-job")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.warehouse.dir", sparkWarehouseDir)
    .config("spark.eventLog.enabled", value = true)
    .config("spark.eventLog.dir", sparkEventLogDir)
    .enableHiveSupport
    .getOrCreate()
  val sparkContext = sparkSession.sparkContext
  val sqlContext = sparkSession.sqlContext
  logger.info("*** Initialized Spark instance.")

  sys.addShutdownHook {
    sparkSession.stop()
    logger.info("*** Terminated Spark instance.")
  }
}