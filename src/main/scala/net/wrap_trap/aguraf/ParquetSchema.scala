package net.wrap_trap.aguraf

import java.nio.file.{FileSystems, Files, Path}

import org.apache.calcite.schema.{Table, TranslatableTable}
import org.apache.calcite.schema.impl.AbstractSchema

import net.wrap_trap.aguraf.Helper._

class ParquetSchema(val path: Path) extends AbstractSchema {

  override def getTableMap(): java.util.Map[String, Table] = {
    val tableNames = scala.collection.mutable.Set[(String, Path)]()

    val matcher = FileSystems.getDefault().getPathMatcher("glob:**/*.parquet")
    Files.walk(this.path).filter(matcher.matches(_)).forEach(p => tableNames += getTablePath(p))

    tableNames.foldLeft(new java.util.HashMap[String, Table]()) { case (acc, (name, dirPath)) =>
      acc.put(name, createTranslatableTable(name, dirPath))
      acc
    }
  }

  protected def createTranslatableTable(tableName: String, dirPath: Path): TranslatableTable = {
    return null
  }

  protected def getTablePath(p: Path): (String, Path) = {
    val len: Int = p.getNameCount
    return (p.getName(len - 2).toString.toUpperCase, p.getParent)
  }
}
