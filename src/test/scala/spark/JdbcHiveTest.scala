package spark

import org.apache.spark.sql.{Dataset, Encoders, SaveMode}
import org.scalatest.{FunSuite, Matchers}

case class KeyValue(key: Int, value: Int)

object KeyValue {
  val keyValueSchema = Encoders.product[KeyValue].schema
}

class JdbcHiveTest extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  test("jdbc-to-hive") {
    val keyvalues = prepareSource()
    prepareSink()

    sqlContext
      .read
      .format("jdbc")
      .option("driver", "org.h2.Driver")
      .option("url", "jdbc:h2:mem:kv;DB_CLOSE_DELAY=-1")
      .option("user", "sa")
      .option("password", "sa")
      .option("dbtable", "jdbc_keyvalue")
      .load
      .as[KeyValue]
      .map(keyvalue => keyvalue.copy(value = keyvalue.value + 1))
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("hive_keyvalue")

    keyvalues.createOrReplaceTempView("jdbc_keyvalue")
    sqlContext.sql("select * from jdbc_keyvalue order by key").show
    sqlContext.sql("select * from hive_keyvalue order by key").show
  }

  private def prepareSource(): Dataset[KeyValue] = {
    val keyvalues = List[KeyValue](KeyValue(1, 1), KeyValue(2, 2), KeyValue(3, 3)).toDS
    keyvalues
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", "org.h2.Driver")
      .option("url", "jdbc:h2:mem:kv;DB_CLOSE_DELAY=-1")
      .option("user", "sa")
      .option("password", "sa")
      .option("dbtable", "jdbc_keyvalue")
      .save
    keyvalues
  }

  private def prepareSink(): Unit = {
    sqlContext.sql("DROP TABLE IF EXISTS hive_keyvalue")
    sqlContext.sql("CREATE TABLE hive_keyvalue (key INT, value INT) row format delimited fields terminated by ','")
    ()
  }
}