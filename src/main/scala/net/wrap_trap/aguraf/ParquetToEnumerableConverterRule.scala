package net.wrap_trap.aguraf

import org.apache.calcite.adapter.enumerable.EnumerableConvention
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule

object ParquetToEnumerableConverterRule {
  val INSTANCE = new ParquetToEnumerableConverterRule()
}

class ParquetToEnumerableConverterRule
  extends ConverterRule(classOf[RelNode], ParquetRel.CONVENTION, EnumerableConvention.INSTANCE, "ParquetToEnumerableConvertRule") {
  override def convert(rel: RelNode): RelNode = {
    val newTraitSet = rel.getTraitSet().replace(getOutConvention)
    new ParquetToEnumerableConverter(rel.getCluster, newTraitSet, rel)
  }
}
