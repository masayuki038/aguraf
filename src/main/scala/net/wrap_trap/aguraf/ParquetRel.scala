package net.wrap_trap.aguraf

import org.apache.calcite.plan.{RelOptTable, Convention}
import org.apache.calcite.rel.RelNode

trait ParquetRel extends RelNode {
  val CONVENTION = new Convention.Impl("PARQUET", classOf[ParquetRel])

  def implement(parquetContext: ParquetContext): Unit = ???

}

class ParquetContext(val table: RelOptTable, val parquetTable: ParquetTranslatableTable, val limit: Int) {
  val selectedField = scala.collection.mutable.Map[String, String]()
  val whereClause = scala.collection.mutable.Seq[String]()
  val orderByFields = scala.collection.mutable.Seq[String]()
  val groupByFields = scala.collection.mutable.Seq[String]()
  val aggregateFunctions = scala.collection.mutable.Map[String, String]()

  def visitChild(input: RelNode): Unit = {
    input.asInstanceOf[ParquetRel].implement(this)
  }

  override def toString(): String = {
    s"""
       ParquetContext {
         selectFields = $selectedField
         , whereClause = $whereClause,
         , orderByFields = $orderByFields,
         , limit = $limit
         , groupByFields = $groupByFields
         , table = $table
         , parquetTable = $parquetTable
       }
    """
  }
}