package example.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}

/**
  * UDFs to be compared with sql function equivalents.
  */
object functions {

  def maxInt(col: String): UserDefinedAggregateFunction = new UserDefinedAggregateFunction {

    override def inputSchema: StructType =
      StructType(StructField(col, IntegerType, nullable = false) :: Nil)

    override def bufferSchema: StructType =
      StructType(StructField(col, IntegerType, nullable = false) :: Nil)

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

    override def evaluate(buffer: Row): Any = buffer.getInt(0)
  }

  
}
