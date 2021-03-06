package spark

import org.apache.spark.sql.Encoders

case class KeyValue(key: Int, value: Int)

object KeyValue {
  val keyValueSchema = Encoders.product[KeyValue].schema
}