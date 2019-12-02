package spark

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object JdbcHiveJob {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("*** Required args(0) = Jdbc Url and args(1) = Hive Table ***")
      System.exit(-1)
    }

    val logger = Logger.getLogger(getClass.getSimpleName)
    val conf = ConfigFactory.load("job.conf").getConfig("job")

    val jdbcUrl = args(0)
    val hiveTable = args(1)
    logger.info(s"Jdbc Url: $jdbcUrl")
    logger.info(s"Hive Table: $hiveTable")

    makeSparkEventLogDir(conf.getString("spark.eventLog.dir"))
    runJob(logger, conf, jdbcUrl, hiveTable)
  }

  def runJob(logger: Logger, conf: Config, jdbcUrl: String, hiveTable: String): Unit = {
    val sparkConf = new SparkConf()
      .setMaster(conf.getString("master"))
      .setAppName(conf.getString("name"))
      .set("spark.serializer", conf.getString("spark.serializer"))
      .set("spark.eventLog.enabled", conf.getBoolean("spark.eventLog.enabled").toString)
      .set("spark.eventLog.dir", conf.getString("spark.eventLog.dir"))

    val sparkSession = SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate()
    logger.info("*** JdbcHiveJob Spark session built. Press Ctrl C to stop.")

    sys.addShutdownHook {
      sparkSession.stop
      logger.info("*** JdbcHiveJob Spark session stopped.")
    }

    // import sparkSession.implicits._

    // Build spark job.

    ()
  }
}