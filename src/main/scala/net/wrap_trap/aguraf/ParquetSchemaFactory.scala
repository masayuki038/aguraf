package net.wrap_trap.aguraf

import java.nio.file.{Paths, Path}
import org.apache.calcite.schema.{SchemaPlus, Schema, SchemaFactory}

class ParquetSchemaFactory extends SchemaFactory {
  def create(parentSchema: SchemaPlus, name: String, operand: java.util.Map[String, AnyRef]): Schema = {
    val path: Path = Paths.get(operand.get("directory").asInstanceOf[String])
    return new ParquetSchema(path)
  }
}
