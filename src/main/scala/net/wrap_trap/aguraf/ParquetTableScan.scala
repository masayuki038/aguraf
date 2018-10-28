package net.wrap_trap.aguraf

import com.typesafe.scalalogging.LazyLogging
import org.apache.calcite.adapter.enumerable.EnumerableRel.{Result, Prefer}
import org.apache.calcite.adapter.enumerable.{EnumerableRelImplementor, EnumerableRel, EnumerableConvention}
import org.apache.calcite.plan.{RelOptPlanner, RelOptCluster, RelOptTable}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan

class ParquetTableScan(cluster: RelOptCluster, tablex: RelOptTable, projectRowType: Option[RelDataType])
  extends TableScan(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), tablex)
  with EnumerableRel with LazyLogging {

  override def deriveRowType(): RelDataType = {
    this.projectRowType match {
      case Some(t) => t
      case _ => super.deriveRowType()
    }
  }

  override def register(planner: RelOptPlanner): Unit = {
    planner.addRule(ParquetToEnumerableConverterRule.INSTANCE)
    ParquetRules.RULES.foreach(_ => planner.addRule(_))
  }

  override def implement(implementor: EnumerableRelImplementor, pref: Prefer): Result = {
    ???
  }
}
