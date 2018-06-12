package com.tech1900.spark.sql

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class RangePartitionStrategyTest extends FunSuite{
  test("applyRangeParition"){
    import PartitionHelper.SQLContextImprovements
    val noOfCores = 5
    val spark = SparkSession.builder().master(s"local[$noOfCores]")
      .config("spark.sql.shuffle.partitions", noOfCores)
      .getOrCreate()
    spark.sqlContext.withRangeParitionAggregation()
    spark.range(0,Int.MaxValue).createTempView("data")
    val frame = spark.sql(
      """
        | SELECT id%10 key,count(id)
        | FROM data
        | GROUP BY 1
      """.stripMargin)
    frame.explain(true)
    frame.show(10,truncate = false)
    spark.sqlContext.removeRangeParitionAggregation()
  }
}
