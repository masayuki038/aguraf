package net.wrap_trap.aguraf

import com.typesafe.scalalogging.LazyLogging
import org.apache.calcite.adapter.enumerable.EnumerableRel.{Result, Prefer}
import org.apache.calcite.adapter.enumerable.{EnumerableRelImplementor, EnumerableRel, EnumerableConvention}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable}
import org.apache.calcite.rel.core.TableScan

class ParquetTableScan(cluster: RelOptCluster, tablex: RelOptTable)
  extends TableScan(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), tablex)
  with EnumerableRel with LazyLogging {
  override def implement(implementor: EnumerableRelImplementor, pref: Prefer): Result = {
    ???
  }
}
