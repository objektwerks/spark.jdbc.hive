package spark

import java.io.File

import org.apache.spark.sql.SparkSession

object SparkInstance {
  private val logger = Logger.getLogger(getClass.getSimpleName)

  val sparkWarehouseDir = new File("./target/spark-warehouse").getAbsolutePath
  val sparkEventLogDir = "/tmp/spark-events"
  val sparkEventDirCreated = makeSparkEventLogDir(sparkEventLogDir)
  logger.info(s"*** $sparkEventLogDir exists or was created: $sparkEventDirCreated")

  val sparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("spark-app")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.warehouse.dir", sparkWarehouseDir)
    .config("spark.eventLog.enabled", true)
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