package example.test.utils

import org.apache.spark.sql.SparkSession

object SparkRunner {

  def runJob(testFn: SparkSession => Unit): Unit = {

    // reduce logging output
    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark: SparkSession = SparkSession.builder().master("local[1]").appName("TestRunner").getOrCreate()

    try {
      testFn(spark)
    } catch {
      case ex: Throwable => assert(assertion = false, s"threw exception during test: ${ex.getMessage}")
        ex.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
