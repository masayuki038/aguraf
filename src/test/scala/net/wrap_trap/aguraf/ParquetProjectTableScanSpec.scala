package net.wrap_trap.aguraf

import java.net.URLDecoder
import java.nio.file.FileSystems
import java.sql.DriverManager
import java.util.Properties;

import net.wrap_trap.aguraf.Helper._
import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.plan.{RelOptTable, RelOptCluster}

import org.apache.calcite.rel.`type`.RelDataTypeFactory.FieldInfoBuilder
import org.mockito.Mockito._
import org.scalatest.FunSpec
import org.scalatest.Matchers._

import scala.collection.mutable.ListBuffer

class ParquetProjectTableScanSpec extends FunSpec {

  describe("ParquetTableScan") {
    it("should return all records") {
      val results = sql("select * from SAMPLES", "samples")
      results.length shouldBe 10
    }
  }

  private def jsonPath(model: String): String = {
    resourcePath(model + ".json")
  }

  private def resourcePath(path: String): String = {
    val url = this.getClass.getResource("/" + path)
    val s = URLDecoder.decode(url.toString, "UTF-8")
    if (s.startsWith("file:")) {
      s.substring("file:".length)
    } else {
      s
    }
  }

  private def sql(sql: String, model: String): Seq[Seq[Any]] = {
    val info = new Properties()
    info.put("model", jsonPath(model))
    using(DriverManager.getConnection("jdbc:calcite:", info)) { connection =>
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery(sql)
      val metaData = resultSet.getMetaData()
      val column = metaData.getColumnCount
      var ret = new ListBuffer[Seq[Any]]()
      while(resultSet.next()) {
        val row = (1 to column).map(i => resultSet.getObject(i))
        ret += row
      }
      ret
    }
  }

  describe("ParquetTableScan") {
    it("should return values of unionCol") {
      val typeFactory = new JavaTypeFactoryImpl(org.apache.calcite.rel.`type`.RelDataTypeSystem.DEFAULT)
      val relDataType = typeFactory.createJavaType(classOf[Int])
      val fieldBuilder = new FieldInfoBuilder(typeFactory)
      fieldBuilder.add("unionCol", relDataType)

      val tableScan = new ParquetTableScan(
        mock(classOf[RelOptCluster]),
        mock(classOf[RelOptTable]),
        Option(fieldBuilder.build()),
        FileSystems.getDefault().getPath("src/test/resources/samples"))

      val context = tableScan.implement(new ParquetContext())
      context.vectorSchemaRoots.isDefined shouldBe true
      val vectorSchemaRoots = context.vectorSchemaRoots.get
      vectorSchemaRoots.length shouldBe 2
      vectorSchemaRoots(0).getRowCount shouldBe 4
      vectorSchemaRoots(0).getFieldVectors.size shouldBe 1
      vectorSchemaRoots(1).getRowCount shouldBe 4
      vectorSchemaRoots(1).getFieldVectors.size shouldBe 1
    }

    it("should return values of (unionCol, simpleCol") {
      val typeFactory = new JavaTypeFactoryImpl(org.apache.calcite.rel.`type`.RelDataTypeSystem.DEFAULT)
      val typeOfInt = typeFactory.createJavaType(classOf[Int])
      val typeOfDouble = typeFactory.createJavaType(classOf[Double])
      val fieldBuilder = new FieldInfoBuilder(typeFactory)
      fieldBuilder.add("unionCol", typeOfInt)
      fieldBuilder.add("simpleCol", typeOfDouble)

      val tableScan = new ParquetTableScan(
        mock(classOf[RelOptCluster]),
        mock(classOf[RelOptTable]),
        Option(fieldBuilder.build()),
        FileSystems.getDefault().getPath("src/test/resources/samples"))

      val context = tableScan.implement(new ParquetContext())
      context.vectorSchemaRoots.isDefined shouldBe true
      val vectorSchemaRoots = context.vectorSchemaRoots.get
      vectorSchemaRoots.length shouldBe 2
      vectorSchemaRoots(0).getRowCount shouldBe 4
      vectorSchemaRoots(0).getFieldVectors.size shouldBe 2
      vectorSchemaRoots(1).getRowCount shouldBe 4
      vectorSchemaRoots(1).getFieldVectors.size shouldBe 2
    }
  }
}
