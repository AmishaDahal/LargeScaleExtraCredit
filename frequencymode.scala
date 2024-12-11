import org.apache.spark.sql.expressions.{
  MutableAggregationBuffer,
  UserDefinedAggregateFunction
}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import scala.collection.mutable
import scala.collection.Map

class MostFrequentValueUDAF extends UserDefinedAggregateFunction {

  // Input schema: one column of type String
  def inputSchema: StructType = StructType(
    StructField("inputValue", StringType) :: Nil
  )

  // Buffer schema: one column holding a map of String -> Long
  def bufferSchema: StructType = StructType(
    StructField("valueCounts", MapType(StringType, LongType)) :: Nil
  )

  // Return schema: two fields: the most frequent value (String) and its frequency (Long)
  def dataType: DataType = StructType(
    StructField("mostFrequentValue", StringType) :: StructField(
      "frequency",
      LongType
    ) :: Nil
  )

  // The UDAF is deterministic since the mode and frequency are deterministically computed
  def deterministic: Boolean = true

  // Initialize the buffer with an empty map
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map.empty[String, Long]
  }

  // Update the buffer with the current input value, ignoring empty strings
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val currentMap = buffer.getMap[String, Long](0)
    val value = input.getString(0)
    if (value.trim.nonEmpty) { // Filter out empty strings
      val currentCount = currentMap.getOrElse(value, 0L)
      buffer(0) = currentMap + (value -> (currentCount + 1L))
    }
  }

  // Merge two buffers together by combining their maps
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val map1 = buffer1.getMap[String, Long](0)
    val map2 = buffer2.getMap[String, Long](0)
    val merged = map1 ++ map2.map { case (key, value) =>
      key -> (value + map1.getOrElse(key, 0L))
    }
    buffer1(0) = merged
  }

  // Finalize the aggregation and return the most frequent value and its frequency
  def evaluate(buffer: Row): Row = {
    val valueCounts: Map[String, Long] = buffer.getMap[String, Long](0)

    if (valueCounts.isEmpty) {
      Row("", 0L) // Handle empty input
    } else {
      val (mostFrequentValue, freq) = valueCounts.maxBy(_._2)
      Row(mostFrequentValue, freq)
    }
  }
}