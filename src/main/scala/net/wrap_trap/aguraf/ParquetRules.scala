package net.wrap_trap.aguraf

import org.apache.calcite.rel.`type`.RelDataTypeFactory.FieldInfoBuilder
import org.apache.calcite.rex.RexInputRef

import collection.JavaConverters._

import org.apache.calcite.plan.{RelOptRuleCall, RelOptRule}
import org.apache.calcite.rel.core.RelFactories
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.tools.RelBuilderFactory

object ParquetRules {
  val RULES: Seq[ParquetRel] = Seq()
}

class ParquetRules {}


object ParquetProjectTableScanRule {
  val INSTANCE = new ParquetProjectTableScanRule(RelFactories.LOGICAL_BUILDER)
}

class ParquetProjectTableScanRule(relBuilderFactory: RelBuilderFactory)
  extends RelOptRule(
    RelOptRule.operand(classOf[LogicalProject], RelOptRule.operand(classOf[ParquetTableScan], RelOptRule.none())),
    relBuilderFactory,
    "ParquetProjectTableScanRule"
  ) {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val project: LogicalProject = call.rel(0)
    val scan: ParquetTableScan = call.rel(1)

    val fieldBuilder = new FieldInfoBuilder(scan.getCluster.getTypeFactory)
    project.getProjects().asScala.foreach(s => s match {
      case inputRef: RexInputRef => fieldBuilder.add(inputRef.getName, inputRef.getType)
      case _ => throw new IllegalArgumentException("Unexpected type") // not a simple projection
    })
    call.transformTo(
      new ParquetTableScan(
        scan.getCluster,
        scan.getTable,
        Option(fieldBuilder.build()),
        scan.dirPath
      )
    )
  }
}