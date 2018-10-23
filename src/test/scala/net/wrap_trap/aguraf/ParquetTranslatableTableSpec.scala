package net.wrap_trap.aguraf

import java.nio.file.Paths
import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.sql.`type`.{SqlTypeName, BasicSqlType}
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

import collection.JavaConverters._

import org.scalatest.FunSpec
import org.scalatest.Matchers._

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._

class ParquetTranslatableTableSpec extends FunSpec {

  describe("ParquetTranslatableTable") {
    it("should read column descriptors from parquet files in SAMPLES") {
      val dirPath = Paths.get("src/test/resources/samples").toAbsolutePath()
      val columns = ParquetTranslatableTable("SAMPLES", dirPath).columns
      val paths = columns.map(_.getPath).flatten
      paths should contain("unionCol")
      paths should contain("simpleCol")
      columns.filter(c => c.getPath().contains("unionCol")).foreach {
        _.getPrimitiveType.getPrimitiveTypeName should be(INT32)
      }
      columns.filter(c => c.getPath().contains("simpleCol")).foreach {
        _.getPrimitiveType.getPrimitiveTypeName should be(DOUBLE)
      }
    }

    it("should read column descriptors from parquet files in USERDATA") {
      val dirPath = Paths.get("src/test/resources/userdata").toAbsolutePath()
      val columns = ParquetTranslatableTable("USERDATA", dirPath).columns
      val paths = columns.map(_.getPath).flatten
      paths should contain("registration_dttm")

      columns.filter(c => c.getPath().contains("registration_dttm")).foreach {
        _.getPrimitiveType.getPrimitiveTypeName should be(INT96)
      }
    }

    it("should get correct RelDataType") {
      val columns = Array(
        new ColumnDescriptor(Array("column_int"), PrimitiveTypeName.INT32, 1, 1),
        new ColumnDescriptor(Array("column_bigint"), PrimitiveTypeName.INT64, 1, 1),
        new ColumnDescriptor(Array("column_varchar"), PrimitiveTypeName.BINARY, 1, 1),
        new ColumnDescriptor(Array("column_float"), PrimitiveTypeName.FLOAT, 1, 1),
        new ColumnDescriptor(Array("column_double"), PrimitiveTypeName.DOUBLE, 1, 1)
      )
      val table = ParquetTranslatableTable("SAMPLES", columns)
      val relDataType = table.getRowType(new JavaTypeFactoryImpl())
      val intField = relDataType.getField("column_int", false, false)
      intField.getName should be ("COLUMN_INT")
      intField.getType.getSqlTypeName should be (SqlTypeName.INTEGER)
      val bigIntField = relDataType.getField("column_bigint", false, false)
      bigIntField.getName should be ("COLUMN_BIGINT")
      bigIntField.getType.getSqlTypeName should be (SqlTypeName.BIGINT)
      val varcharField = relDataType.getField("column_varchar", false, false)
      varcharField.getName should be ("COLUMN_VARCHAR")
      varcharField.getType.getSqlTypeName should be (SqlTypeName.VARCHAR)
      val floatField = relDataType.getField("column_float", false, false)
      floatField.getName should be ("COLUMN_FLOAT")
      floatField.getType.getSqlTypeName should be (SqlTypeName.FLOAT)
      val doubleField = relDataType.getField("column_double", false, false)
      doubleField.getName should be ("COLUMN_DOUBLE")
      doubleField.getType.getSqlTypeName should be (SqlTypeName.DOUBLE)
    }
  }
}
