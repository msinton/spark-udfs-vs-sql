package example.udf

import org.scalatest.FunSuite
import example.test.utils._

class functionsTest extends FunSuite {

  test("maxInt works as expected") {

    import example.udf.functions._
    import SparkRunner._

    runJob { spark =>
      import spark.implicits._

      val df = (0 to 100).toDF("value")

      val maxUdaf = maxInt("value")

      val result = df.agg(maxUdaf('value)).head().toSeq

      assert(result === Seq(100))
    }

  }

}
