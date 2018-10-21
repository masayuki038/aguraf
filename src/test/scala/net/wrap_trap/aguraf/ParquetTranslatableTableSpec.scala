package net.wrap_trap.aguraf

import java.nio.file.Paths
import collection.JavaConverters._

import org.scalatest.FunSpec
import org.scalatest.Matchers._

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._

class ParquetTranslatableTableSpec extends FunSpec {

  describe("ParquetTranslatableTable") {
    it ("should get columns in parquet files") {
      val dirPath = Paths.get("src/test/resources/samples").toAbsolutePath()
      val columns = new ParquetTranslatableTable("SAMPLES", dirPath, null).columns.asScala
      val paths = columns.map(_.getPath).flatten
      paths should contain ("unionCol")
      paths should contain ("simpleCol")
      columns.filter(c => c.getPath().contains("unionCol")).foreach {
        _.getPrimitiveType.getPrimitiveTypeName should be (INT32)
      }
      columns.filter(c => c.getPath().contains("simpleCol")).foreach {
        _.getPrimitiveType.getPrimitiveTypeName should be (DOUBLE)
      }
    }
  }
}
