package net.wrap_trap.aguraf

import java.nio.file.{Paths, Path}
import java.security.CodeSource

import org.scalatest.FunSpec
import org.scalatest.Matchers._

class ParquetSchemaSpec extends FunSpec {
  describe("ParquetSchema") {
    it("should contain the table of SAMPLES") {
      val path = Paths.get("src/test/resources").toAbsolutePath
      new ParquetSchema(path).getTableMap() should contain key ("SAMPLES")
    }
  }
}
