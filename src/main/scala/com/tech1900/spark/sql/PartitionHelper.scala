package com.tech1900.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.physical.RangePartitionStrategy

object PartitionHelper {
  implicit class SQLContextImprovements(val sqlContext: SQLContext) {
    /**
      * Apply Range Partitioner for aggregating highly skewed data
      * 1) Sample the data and determine partition boundaries
      * 2) Do aggregation
      * @return
      */
    def withRangeParitionAggregation(): SQLContext ={
      sqlContext.experimental.extraStrategies = RangePartitionStrategy(sqlContext) :: Nil
      sqlContext
    }
    def removeRangeParitionAggregation(): SQLContext ={
      if(sqlContext.experimental.extraStrategies.nonEmpty){
        sqlContext.experimental.extraStrategies =
          sqlContext.experimental.extraStrategies.filter(x=> !x.isInstanceOf[RangePartitionStrategy])
      }
      sqlContext
    }
  }
}
