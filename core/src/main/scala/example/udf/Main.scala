package example.udf

import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    try {
      println("do something")
    } finally {
      spark.stop()
    }
  }
}
