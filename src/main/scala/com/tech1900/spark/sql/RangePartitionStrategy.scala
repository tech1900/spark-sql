package org.apache.spark.sql.catalyst.plans.physical {

  import org.apache.spark.sql.catalyst.expressions.{Ascending, Expression, SortOrder}
  import org.apache.spark.sql.catalyst.planning.PhysicalAggregation
  import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
  import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
  import org.apache.spark.sql.execution.{SparkPlan, SparkPlanner, UnaryExecNode}
  import org.apache.spark.sql.{SQLContext, Strategy}

  case class AggregationPlanner(sqlContext: SQLContext)
        extends SparkPlanner(sqlContext.sparkContext,sqlContext.conf,sqlContext.experimental) {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      Aggregation(plan)
    }
  }

  case class RangePartitionStrategy(sqlContext: SQLContext) extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      plan match{
        case PhysicalAggregation(_, _, _, _)=>
          val plans = AggregationPlanner(sqlContext)(plan)
          plans.map {
            case node: UnaryExecNode =>
              applyShufflePartition(node)
            case x =>
              x
          }
        case _=>
          Nil
      }
    }

    private def applyShufflePartition(node: UnaryExecNode): UnaryExecNode = {
      val child = node.getClass.getDeclaredField("child")
      child.setAccessible(true)
      child.set(node, ShuffleExchangeExec(getRangePartitioning(node.requiredChildDistribution.head), node.child))
      node
    }

    /**
      * Force RangeParitioning for all types
      *
      * @param distribution
      * @return
      */
    private def getRangePartitioning(distribution: Distribution) = {
      val numPartitions = distribution.requiredNumPartitions.getOrElse(sqlContext.conf.numShufflePartitions)
      distribution match {
        case ClusteredDistribution(clustering,requiredNumPartitions) =>
          RangePartitioning(clustering.map(x=>SortOrder(x,Ascending)), numPartitions)
        case HashClusteredDistribution(expressions)=>
          RangePartitioning(expressions.map(x=>SortOrder(x,Ascending)), numPartitions)
        case OrderedDistribution(ordering) =>
          RangePartitioning(ordering, numPartitions)
        case _=>
          distribution.createPartitioning(numPartitions)
      }
    }
  }
}