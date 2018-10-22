package net.wrap_trap.aguraf

import java.lang.reflect.Type
import java.nio.file.{Files, Path}

import org.apache.calcite.rel.`type`.RelDataTypeFactory.FieldInfoBuilder
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

import collection.JavaConverters._

import com.typesafe.scalalogging.LazyLogging
import org.apache.calcite.linq4j.tree.Expression
import org.apache.calcite.linq4j.{Queryable, QueryProvider}
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.plan.RelOptTable.ToRelContext
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.schema.{SchemaPlus, TranslatableTable, QueryableTable}
import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.fs.{Path => HDFSPath}
import org.apache.parquet.format.converter.ParquetMetadataConverter

object ParquetTranslatableTable extends LazyLogging {

  def apply(tableName: String, columns: Seq[ColumnDescriptor]): ParquetTranslatableTable = {
    new ParquetTranslatableTable(tableName, columns)
  }

  def apply(tableName: String, dirPath: Path): ParquetTranslatableTable = {
    new ParquetTranslatableTable(tableName, getColumns(dirPath))
  }

  private def getColumns(dirPath: Path): Seq[ColumnDescriptor] = {
    val parquetFiles = Files.newDirectoryStream(dirPath, "*.parquet").asScala.toSeq
    val hdfsPath = new HDFSPath(parquetFiles(0).toAbsolutePath().toString())
    val config = new Configuration()
    logger.debug(s"Reading Parquet schema from $dirPath")
    val metadata = ParquetFileReader.readFooter(config, hdfsPath, ParquetMetadataConverter.NO_FILTER)
    val messageType = metadata.getFileMetaData.getSchema()
    messageType.getColumns.asScala
    // TODO Check all parquet files have same column descriptors
  }
}

class ParquetTranslatableTable(val tableName: String, val columns: Seq[ColumnDescriptor])
  extends AbstractTable with QueryableTable with TranslatableTable with LazyLogging {

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    val fieldBuilder = new FieldInfoBuilder(typeFactory)
    columns.foreach { descriptor =>
      assert(descriptor.getPath.size == 1)
      val name = descriptor.getPath.apply(0).toUpperCase
      val columnType = descriptor.getType match {
        case PrimitiveTypeName.FLOAT => typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.FLOAT), true)
        case PrimitiveTypeName.BINARY => typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true)
        case PrimitiveTypeName.INT32 => typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        case PrimitiveTypeName.INT64 => typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true)
        case PrimitiveTypeName.DOUBLE => typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true)
      }
      fieldBuilder.add(name, columnType)
    }
    fieldBuilder.build()
  }

  override def getExpression(schema: SchemaPlus, tableName: String, clazz: Class[_]): Expression = ???

  override def getElementType: Type = ???

  override def asQueryable[T](queryProvider: QueryProvider, schema: SchemaPlus, tableName: String): Queryable[T] = ???

  override def toRel(context: ToRelContext, relOptTable: RelOptTable): RelNode = ???
}
