package net.wrap_trap.aguraf

import java.lang.reflect.Type
import java.nio.file.{Files, Path}

import collection.JavaConverters._

import com.typesafe.scalalogging.LazyLogging
import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.linq4j.tree.Expression
import org.apache.calcite.linq4j.{Queryable, QueryProvider}
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.plan.RelOptTable.ToRelContext
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.{RelProtoDataType, RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.schema.{SchemaPlus, TranslatableTable, QueryableTable}
import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.fs.{Path => HDFSPath}
import org.apache.parquet.format.converter.ParquetMetadataConverter

class ParquetTranslatableTable(val tableName: String, val dirPath: Path, val tProtoRowType: RelProtoDataType)
  extends AbstractTable with QueryableTable with TranslatableTable with LazyLogging {

  val columns = {
    val parquetFiles = Files.newDirectoryStream(dirPath, "*.parquet").asScala.toSeq
    val hdfsPath = new HDFSPath(parquetFiles(0).toAbsolutePath().toString())
    val config = new Configuration()
    logger.debug(s"Reading Parquet schema from $dirPath")
    val metadata = org.apache.parquet.hadoop.ParquetFileReader.readFooter(config, hdfsPath, ParquetMetadataConverter.NO_FILTER)
    val messageType = metadata.getFileMetaData.getSchema()
    messageType.getColumns
    // TODO Check all parquet files have same column descriptors
  }

  def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    if (this.tProtoRowType != null) {
      return this.tProtoRowType.apply(typeFactory)
    }
    deduceRowType(null, typeFactory.asInstanceOf[JavaTypeFactory])
  }

  def deduceRowType(a: ParquetSchema, typeFactory: JavaTypeFactory): RelDataType = {
    // parquetのSchemaをreadして、それに対する(fieldName, RelDataType)を返す
    // githubにあるcalcite+parquetのものを参考にする
    return null
  }

  override def getExpression(schema: SchemaPlus, tableName: String, clazz: Class[_]): Expression = ???

  override def getElementType: Type = ???

  override def asQueryable[T](queryProvider: QueryProvider, schema: SchemaPlus, tableName: String): Queryable[T] = ???

  override def toRel(context: ToRelContext, relOptTable: RelOptTable): RelNode = ???
}
