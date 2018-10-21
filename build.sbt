name := "aguraf"

version := "1.0"

scalaVersion := "2.12.7"

libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.0.6"
libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.12.7"
libraryDependencies += "org.apache.calcite" % "calcite-core" % "1.17.0"
libraryDependencies += "org.apache.calcite" % "calcite-linq4j" % "1.17.0"
libraryDependencies += "org.apache.parquet" % "parquet-common" % "1.9.0"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.7"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.7"
libraryDependencies += "ch.qos.logback" % "logback-core" % "1.2.3"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"
libraryDependencies += "org.apache.parquet" % "parquet-common" % "1.10.0"