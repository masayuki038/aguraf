package net.wrap_trap.aguraf

import java.nio.file.{Files, Path}

import net.wrap_trap.parquet_to_arrow.ParquetToArrow

import collection.JavaConverters._

import com.typesafe.scalalogging.LazyLogging

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.calcite.adapter.enumerable.{EnumerableConvention}
import org.apache.calcite.plan.{RelOptPlanner, RelOptCluster, RelOptTable}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan

class ParquetTableScan(cluster: RelOptCluster, tablex: RelOptTable, projectRowType: Option[RelDataType], val dirPath: Path)
  extends TableScan(cluster, cluster.traitSetOf(ParquetRel.CONVENTION), tablex)
  with ParquetRel with LazyLogging {

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

  override def implement(parquetContext: ParquetContext): ParquetContext = {
    val projectedFieldNames =  deriveRowType().getFieldNames
    val converter = new ParquetToArrow()

    val parquetFiles = Files.newDirectoryStream(this.dirPath, "*.parquet").asScala.toSeq
    val vectorSchemaRoots = parquetFiles.map { parquetFile =>
      val v = converter.convert(parquetFile.toAbsolutePath.toString)
      // TODO do projection in ParquetToArrow
      project(v, projectedFieldNames)
    }
    parquetContext.vectorSchemaRoots(vectorSchemaRoots)
  }

  private def project(v: VectorSchemaRoot, fieldNames: java.util.List[String]): VectorSchemaRoot = {
    val fieldVectors  = v.getFieldVectors.asScala.filter(f => fieldNames.contains(f.getField.getName))
    val fields = fieldVectors.map(f => f.getField)
    new VectorSchemaRoot(fields.asJava, fieldVectors.asJava, v.getRowCount)
  }
}
