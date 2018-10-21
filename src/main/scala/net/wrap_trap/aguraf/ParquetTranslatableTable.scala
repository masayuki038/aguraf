package net.wrap_trap.aguraf

import java.lang.reflect.Type
import java.nio.file.Path

import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.linq4j.tree.Expression
import org.apache.calcite.linq4j.{Queryable, QueryProvider}
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.plan.RelOptTable.ToRelContext
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.{RelProtoDataType, RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.calcite.schema.{SchemaPlus, TranslatableTable, QueryableTable}

class ParquetTranslatableTable(val tableName: String, val dirPath: Path, val tProtoRowType: RelProtoDataType) extends AbstractTable with QueryableTable with TranslatableTable {

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
