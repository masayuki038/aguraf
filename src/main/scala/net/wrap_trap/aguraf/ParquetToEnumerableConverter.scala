package net.wrap_trap.aguraf

import org.apache.calcite.adapter.enumerable.EnumerableRel.{Result, Prefer}
import org.apache.calcite.adapter.enumerable.{EnumerableRelImplementor, EnumerableRel}
import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterImpl
import org.apache.calcite.rel.metadata.RelMetadataQuery

class ParquetToEnumerableConverter(cluster: RelOptCluster, traitSet: RelTraitSet, input: RelNode)
  extends ConverterImpl(cluster, ConventionTraitDef.INSTANCE, traitSet, input)
  with EnumerableRel {


  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    super.computeSelfCost(planner, mq).multiplyBy(.1)
  }

  override def implement(implementor: EnumerableRelImplementor, pref: Prefer): Result = ???
}
