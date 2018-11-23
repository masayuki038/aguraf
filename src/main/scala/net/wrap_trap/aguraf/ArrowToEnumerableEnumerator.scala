package net.wrap_trap.aguraf

import collection.JavaConverters._

import org.apache.calcite.linq4j.Enumerator


class ArrowToEnumerableEnumerator(val context: ParquetContext) extends Enumerator[Any] {
  var rootIndex = 0
  var vectorIndex = 0

  override def current(): Any = {
    context.vectorSchemaRoots match {
      case Some(roots) =>
        roots(this.rootIndex).getFieldVectors.asScala.map(fieldVector => fieldVector.getObject(this.vectorIndex))
      case _ => Array()
    }
  }

  override def moveNext(): Boolean = {
    context.vectorSchemaRoots match {
      case Some(roots) => {
        if (this.vectorIndex >= (roots(this.rootIndex).getRowCount - 1)) {
          if (this.rootIndex >= (roots.size - 1)) {
            return false
          }
          this.rootIndex += 1
          this.vectorIndex = 0
        } else {
          this.vectorIndex += 1
        }
        return true
      }
      case _ => false
    }
  }

  override def reset(): Unit = {}
  override def close(): Unit = {}
}
