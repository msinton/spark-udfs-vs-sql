package example.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * UDFs to be compared with sql function equivalents.
  */
object functions {

  def maxInt(col: String): UserDefinedAggregateFunction = new UserDefinedAggregateFunction {

    override def inputSchema: StructType =
      StructType(StructField(col, IntegerType, nullable = false) :: Nil)

    override def bufferSchema: StructType =
      StructType(StructField("max", IntegerType, nullable = false) :: Nil)

    override def dataType: DataType = IntegerType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) =
      Int.MinValue

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit =
      if (input.getInt(0) > buffer.getInt(0))
        buffer(0) = input.getInt(0)

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit =
      if (buffer2.getInt(0) > buffer1.getInt(0))
        buffer1(0) = buffer2(0)

    override def evaluate(buffer: Row): Any = buffer(0)
  }


  def sumInt(col: String): UserDefinedAggregateFunction = new UserDefinedAggregateFunction {

    override def inputSchema: StructType =
      StructType(StructField(col, IntegerType, nullable = false) :: Nil)

    override def bufferSchema: StructType =
      StructType(StructField("sum", LongType, nullable = false) :: Nil)

    override def dataType: DataType = LongType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) =
      0L

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit =
      buffer(0) = buffer.getLong(0) + input.getInt(0)

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit =
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)

    override def evaluate(buffer: Row): Any = buffer(0)
  }


  def countInt(col: String): UserDefinedAggregateFunction = new UserDefinedAggregateFunction {

    override def inputSchema: StructType =
      StructType(StructField(col, IntegerType, nullable = false) :: Nil)

    override def bufferSchema: StructType =
      StructType(StructField("count", LongType, nullable = false) :: Nil)

    override def dataType: DataType = LongType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) =
      0L

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit =
      buffer(0) = buffer.getLong(0) + 1

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit =
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)

    override def evaluate(buffer: Row): Any = buffer(0)
  }


  def meanInt(col: String): UserDefinedAggregateFunction = new UserDefinedAggregateFunction {

    override def inputSchema: StructType =
      StructType(StructField(col, IntegerType, nullable = false) :: Nil)

    override def bufferSchema: StructType =
      StructType(List(
        StructField("count", LongType, nullable = false),
        StructField("sum", LongType, nullable = false)
      ))

    override def dataType: DataType = DoubleType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getLong(0) + 1
      buffer(1) = buffer.getLong(1) + input.getInt(0)
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    override def evaluate(buffer: Row): Any = buffer.getLong(1).toDouble / buffer.getLong(0)
  }

  def stddevInt(mean: Double)(col: String): UserDefinedAggregateFunction = new UserDefinedAggregateFunction {

    override def inputSchema: StructType =
      StructType(StructField(col, IntegerType, nullable = false) :: Nil)

    override def bufferSchema: StructType =
      StructType(List(
        StructField("count", LongType, nullable = false),
        StructField("varSum", DoubleType, nullable = false)
      ))

    override def dataType: DataType = DoubleType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0D
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getLong(0) + 1
      buffer(1) = buffer.getDouble(1) + Math.pow(input.getInt(0) - mean, 2)
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getDouble(1) + buffer2.getDouble(1)
    }

    override def evaluate(buffer: Row): Any = Math.sqrt(buffer.getDouble(1) / (buffer.getLong(0) - 1))
  }
}
