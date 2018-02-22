package example.test.utils

import org.apache.spark.sql.SparkSession

object SparkRunner {

  def runJob(testFn: SparkSession => Unit): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[1]").appName("TestRunner").getOrCreate()

    try {
      testFn(spark)
    } catch {
      case ex: Throwable => assert(false, s"threw exception during test: ${ex.getMessage}")
        ex.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
