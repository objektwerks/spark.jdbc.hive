package spark

import java.util.UUID

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.scalatest.{FunSuite, Matchers}
import spark.entity.{AvgAgeByRole, KeyValue, Person}

import org.apache.spark.sql.Encoders

case class KeyValue(key: Int, value: Int)

object KeyValue {
  val keyValueSchema = Encoders.product[KeyValue].schema
  implicit def keyValueOrdering: Ordering[KeyValue] = Ordering.by(_.key)
}

class JdbcHiveTest extends FunSuite with Matchers {
  import SparkInstance._
  import sparkSession.implicits._

  test("hive") {
    sqlContext.sql("DROP TABLE IF EXISTS keyvalue")
    sqlContext.sql("CREATE TABLE keyvalue (key INT, value INT) row format delimited fields terminated by ','")
    sqlContext.sql("LOAD DATA LOCAL INPATH './target/hive/kv.txt' INTO TABLE keyvalue")
    val keyvalues = sqlContext.sql("SELECT * FROM keyvalue").as[KeyValue].cache
    keyvalues.count shouldBe 9
    keyvalues.filter(_.key == 3).head.value shouldBe 33
    keyvalues.show
  }

  test("jdbc") {
    writeKeyValues("key_values", List[KeyValue](KeyValue(1, 1), KeyValue(2, 2), KeyValue(3, 3)).toDS)
    val keyvalues = readKeyValues("key_values").toDF
    keyvalues.createOrReplaceTempView("key_values")
    sqlContext.sql("select count(*) as total_rows from key_values").head.getLong(0) shouldBe 3
    sqlContext.sql("select min(key) as min_key from key_values").head.getInt(0) shouldBe 1
    sqlContext.sql("select max(value) as max_value from key_values").head.getInt(0) shouldBe 3
    sqlContext.sql("select sum(value) as sum_value from key_values").head.getLong(0) shouldBe 6
  }

  private def writeKeyValues(table: String, keyValues: Dataset[KeyValue]): Unit = {
    keyValues
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", "org.h2.Driver")
      .option("url", "jdbc:h2:mem:kv;DB_CLOSE_DELAY=-1")
      .option("user", "sa")
      .option("password", "sa")
      .option("dbtable", table)
      .save
  }

  private def readKeyValues(table: String): Dataset[KeyValue] = {
    sqlContext
      .read
      .format("jdbc")
      .option("driver", "org.h2.Driver")
      .option("url", "jdbc:h2:mem:kv;DB_CLOSE_DELAY=-1")
      .option("user", "sa")
      .option("password", "sa")
      .option("dbtable", table)
      .load
      .as[KeyValue]
  }
}