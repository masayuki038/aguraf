package net.wrap_trap.aguraf

import java.lang.reflect.Type
import java.nio.file.{Files, Path}

import org.apache.calcite.rel.`type`.RelDataTypeFactory.FieldInfoBuilder
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.parquet.format.{ConvertedType, SchemaElement, Type => SchemaElementType}
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetFileReader}

import collection.JavaConverters._

import com.typesafe.scalalogging.LazyLogging
import org.apache.calcite.linq4j.tree.Expression
import org.apache.calcite.linq4j.{Queryable, QueryProvider}
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.plan.RelOptTable.ToRelContext
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.schema.{Schemas, SchemaPlus, TranslatableTable, QueryableTable}
import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.fs.{Path => HDFSPath}
import org.apache.parquet.format.converter.ParquetMetadataConverter

object ParquetTranslatableTable extends LazyLogging {

  def apply(tableName: String, columns: Seq[SchemaElement]): ParquetTranslatableTable = {
    new ParquetTranslatableTable(tableName, columns)
  }

  def apply(tableName: String, dirPath: Path): ParquetTranslatableTable = {
    new ParquetTranslatableTable(tableName, getColumns(dirPath))
  }

  private def getColumns(dirPath: Path): Seq[SchemaElement] = {
    val parquetFiles = Files.newDirectoryStream(dirPath, "*.parquet").asScala.toSeq
    val hdfsPath = new HDFSPath(parquetFiles(0).toAbsolutePath().toString())
    val config = new Configuration()
    logger.debug(s"Reading Parquet schema from $dirPath")
    val parquetMetadata = ParquetFileReader.readFooter(config, hdfsPath, ParquetMetadataConverter.NO_FILTER)
    val fileMetaData = new ParquetMetadataConverter().toParquetMetadata(ParquetFileWriter.CURRENT_VERSION, parquetMetadata);
    val messageType = parquetMetadata.getFileMetaData.getSchema()
    fileMetaData.getSchema.asScala
    // TODO Check all parquet files have same column descriptors
  }
}

class ParquetTranslatableTable(val tableName: String, val schemaElements: Seq[SchemaElement])
  extends AbstractTable with QueryableTable with TranslatableTable with LazyLogging {

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    val fieldBuilder = new FieldInfoBuilder(typeFactory)
    schemaElements.foreach { schemaElement =>
      val name = schemaElement.getName.toUpperCase
      val columnType = (schemaElement.getType, schemaElement.getConverted_type) match {
        case (SchemaElementType.FLOAT, _) => typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.FLOAT, schemaElement.getPrecision), true)
        case (SchemaElementType.DOUBLE, _) => typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE, schemaElement.getPrecision), true)
        case (SchemaElementType.BYTE_ARRAY, _) => typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR, schemaElement.getPrecision), true)
        case (SchemaElementType.INT32, ConvertedType.DATE) => typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DATE, schemaElement.getPrecision), true)
        case (SchemaElementType.INT32, ConvertedType.TIME_MILLIS) => typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIME, schemaElement.getPrecision), true)
        case (SchemaElementType.INT32, _) => typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER, schemaElement.getPrecision), true)
        case (SchemaElementType.INT64, ConvertedType.TIME_MICROS) => typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIME, schemaElement.getPrecision), true)
        case (SchemaElementType.INT64, ConvertedType.TIMESTAMP_MILLIS) => typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP, schemaElement.getPrecision), true)
        case (SchemaElementType.INT64, ConvertedType.TIMESTAMP_MICROS) => typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP, schemaElement.getPrecision), true)
        case (SchemaElementType.INT64, _) => typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true)
      }
      fieldBuilder.add(name, columnType)
    }
    fieldBuilder.build()
  }

  override def getExpression(schema: SchemaPlus, tableName: String, clazz: Class[_]): Expression = {
    Schemas.tableExpression(schema, getElementType(), tableName, clazz)
  }

  override def getElementType: Type = {
    classOf[Array[Object]]
  }

  override def asQueryable[T](queryProvider: QueryProvider, schema: SchemaPlus, tableName: String): Queryable[T] = ???

  override def toRel(context: ToRelContext, relOptTable: RelOptTable): RelNode = ???
}
