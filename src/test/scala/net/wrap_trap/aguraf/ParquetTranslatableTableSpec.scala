package net.wrap_trap.aguraf

import java.nio.file.Paths
import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.sql.`type`.{SqlTypeName, BasicSqlType}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.format.{ConvertedType, SchemaElement, Type => SchemaElementType}
import collection.JavaConverters._

import org.scalatest.FunSpec
import org.scalatest.Matchers._

class ParquetTranslatableTableSpec extends FunSpec {

  describe("ParquetTranslatableTable") {
    it("should read column descriptors from parquet files in SAMPLES") {
      val dirPath = Paths.get("src/test/resources/samples").toAbsolutePath()
      val schemaElements = ParquetTranslatableTable("SAMPLES", dirPath).schemaElements
      val names = schemaElements.map(_.getName)
      names should contain("unionCol")
      names should contain("simpleCol")
      schemaElements.filter(_.getName == "unionCol").foreach {
        _.getType should be(SchemaElementType.INT32)
      }
      schemaElements.filter(_.getName == "simpleCol").foreach {
        _.getType should be(SchemaElementType.DOUBLE)
      }
    }

    it("should read column descriptors from parquet files in USERDATA") {
      val dirPath = Paths.get("src/test/resources/userdata").toAbsolutePath()
      val schemaElements = ParquetTranslatableTable("USERDATA", dirPath).schemaElements
      val names = schemaElements.map(_.getName)
      names should contain("registration_dttm")

      schemaElements.filter(_.getName == "simpleCol").foreach {
        _.getType should be(SchemaElementType.INT96)
      }
    }

    it("should get correct RelDataType") {
      val schemaElements = Seq(
        createSchema("column_int", SchemaElementType.INT32, null, 0),
        createSchema("column_bigint", SchemaElementType.INT64, null, 0),
        createSchema("column_varchar", SchemaElementType.BYTE_ARRAY, null, 0),
        createSchema("column_float", SchemaElementType.FLOAT, null, 0),
        createSchema("column_double", SchemaElementType.DOUBLE, null, 0),
        createSchema("column_date", SchemaElementType.INT32, ConvertedType.DATE, 0),
        createSchema("column_time_millis", SchemaElementType.INT32, ConvertedType.TIME_MILLIS, 3),
        createSchema("column_time_micros", SchemaElementType.INT64, ConvertedType.TIME_MICROS, 6),
        createSchema("column_timestamp_millis", SchemaElementType.INT64, ConvertedType.TIMESTAMP_MILLIS, 3),
        createSchema("column_timestamp_micros", SchemaElementType.INT64, ConvertedType.TIMESTAMP_MICROS, 6)
      )
      val table = ParquetTranslatableTable("SAMPLES", schemaElements)
      val relDataType = table.getRowType(new JavaTypeFactoryImpl())
      val intField = relDataType.getField("COLUMN_INT", false, false)
      intField.getName should be ("COLUMN_INT")
      intField.getType.getSqlTypeName should be (SqlTypeName.INTEGER)
      val bigIntField = relDataType.getField("COLUMN_BIGINT", false, false)
      bigIntField.getName should be ("COLUMN_BIGINT")
      bigIntField.getType.getSqlTypeName should be (SqlTypeName.BIGINT)
      val varcharField = relDataType.getField("COLUMN_VARCHAR", false, false)
      varcharField.getName should be ("COLUMN_VARCHAR")
      varcharField.getType.getSqlTypeName should be (SqlTypeName.VARCHAR)
      val floatField = relDataType.getField("COLUMN_FLOAT", false, false)
      floatField.getName should be ("COLUMN_FLOAT")
      floatField.getType.getSqlTypeName should be (SqlTypeName.FLOAT)
      val doubleField = relDataType.getField("COLUMN_DOUBLE", false, false)
      doubleField.getName should be ("COLUMN_DOUBLE")
      doubleField.getType.getSqlTypeName should be (SqlTypeName.DOUBLE)
      val dateField = relDataType.getField("COLUMN_DATE", false, false)
      dateField.getName should be ("COLUMN_DATE")
      dateField.getType.getSqlTypeName should be (SqlTypeName.DATE)
      val timeMillisField = relDataType.getField("COLUMN_TIME_MILLIS", false, false)
      timeMillisField.getName should be ("COLUMN_TIME_MILLIS")
      timeMillisField.getType.getSqlTypeName should be (SqlTypeName.TIME)
      timeMillisField.getType.getPrecision should be (3)
      val timeMicrosField = relDataType.getField("COLUMN_TIME_MICROS", false, false)
      timeMicrosField.getName should be ("COLUMN_TIME_MICROS")
      timeMicrosField.getType.getSqlTypeName should be (SqlTypeName.TIME)
      timeMicrosField.getType.getPrecision should be (3) // When specified type is TIME, RelDataTypeSystemImpl#getMaxPrecision return MAX_DATETIME_PRECISION(3)
      val timestampMillis = relDataType.getField("COLUMN_TIMESTAMP_MILLIS", false, false)
      timestampMillis.getName should be ("COLUMN_TIMESTAMP_MILLIS")
      timestampMillis.getType.getSqlTypeName should be (SqlTypeName.TIMESTAMP)
      timestampMillis.getType.getPrecision should be (3)
      val timestampMicros = relDataType.getField("COLUMN_TIMESTAMP_MICROS", false, false)
      timestampMicros.getName should be ("COLUMN_TIMESTAMP_MICROS")
      timestampMicros.getType.getSqlTypeName should be (SqlTypeName.TIMESTAMP)
      timestampMicros.getType.getPrecision should be (3) // When specified type is TIMESTAMP, RelDataTypeSystemImpl#getMaxPrecision return MAX_DATETIME_PRECISION(3)
    }
  }

  private def createSchema(name: String, elementType: SchemaElementType, convertedType: ConvertedType, precision: Int): SchemaElement = {
    val schemaElement = new SchemaElement(name)
    schemaElement.setType(elementType)
    schemaElement.setConverted_type(convertedType)
    schemaElement.setPrecision(precision)
    schemaElement
  }
}
