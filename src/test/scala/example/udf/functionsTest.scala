package example.udf

import org.scalatest.FunSuite
import example.test.utils._
import org.apache.spark.sql
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction

class functionsTest extends FunSuite {

  import example.udf.functions._

  def check(from: Int, to: Int, expected: Any, udaf: String => UserDefinedAggregateFunction): Unit = {
    import SparkRunner._

    runJob { spark =>
      import spark.implicits._

      val df = (from to to).toDF("value")
      val udafUnderTest = udaf("value")

      val result = df.agg(udafUnderTest('value)).head().toSeq.head

      assert(result === expected)
    }
  }

  test("maxInt works as expected") {
    check(from = 0, to = 100, expected = 100, maxInt)
  }

  test("sumInt works as expected") {
    check(from = 0, to = 5, expected = 15, sumInt)
  }

  test("countInt works as expected") {
    check(from = 0, to = 100, expected = 101, countInt)
  }

  test("meanInt works as expected") {
    check(from = 0, to = 100, expected = 50.0, meanInt)
  }

  test("stddevInt works as expected") {
    check(from = 0, to = 10, expected = 3.3166247903554, stddevInt(5.0))
  }

  test("spark equivalents") {
    import SparkRunner._

    runJob { spark =>
      import spark.implicits._

      val df = (0 to 10).toDF("value")

      assert(df.agg(sql.functions.stddev('value)).head().toSeq.head === 3.3166247903554)

      assert(df.agg(sql.functions.mean('value)).head().toSeq.head === 5.0)

      assert(df.agg(sql.functions.max('value)).head().toSeq.head === 10)

      assert(df.agg(sql.functions.sum('value)).head().toSeq.head === 55)

      assert(df.agg(sql.functions.count('value)).head().toSeq.head === 11)
    }
  }

}
