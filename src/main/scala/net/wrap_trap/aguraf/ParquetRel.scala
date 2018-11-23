package net.wrap_trap.aguraf

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.calcite.plan.{RelOptTable, Convention}
import org.apache.calcite.rel.RelNode

object ParquetRel {
  val CONVENTION = new Convention.Impl("PARQUET", classOf[ParquetRel])
}

trait ParquetRel extends RelNode {
  def implement(parquetContext: ParquetContext): ParquetContext = ???
}

class ParquetContext() {

  private var vectors: Option[Seq[VectorSchemaRoot]] = None
//  val fetchedFields = scala.collection.mutable.Map[String, Object]()
//  val selectedField = scala.collection.mutable.Map[String, String]()
//  val whereClause = scala.collection.mutable.Seq[String]()
//  val orderByFields = scala.collection.mutable.Seq[String]()
//  val groupByFields = scala.collection.mutable.Seq[String]()
//  val aggregateFunctions = scala.collection.mutable.Map[String, String]()

  def vectorSchemaRoots(vectorSchemaRoots: Seq[VectorSchemaRoot]): ParquetContext = {
    val copied = copy()
    copied.vectors = Option(vectorSchemaRoots)
    copied
  }

  def vectorSchemaRoots(): Option[Seq[VectorSchemaRoot]] = {
    this.vectors
  }

  def visitChild(input: RelNode): Unit = {
    input.asInstanceOf[ParquetRel].implement(this)
  }

  private def copy(): ParquetContext = {
    val cloned = new ParquetContext()
    cloned.vectors = this.vectors
    cloned
  }

//  override def toString(): String = {
//    s"""
//       ParquetContext {
//         selectFields = $selectedField
//         , whereClause = $whereClause,
//         , orderByFields = $orderByFields,
//         , limit = $limit
//         , groupByFields = $groupByFields
//         , table = $table
//         , parquetTable = $parquetTable
//       }
//    """
//  }
}