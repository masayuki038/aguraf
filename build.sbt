name := "aguraf"

version := "1.0"

scalaVersion := "2.12.7"

resolvers += "palantir" at "https://dl.bintray.com/palantir/releases"
resolvers += "snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.0.6"
libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.12.7"
libraryDependencies += "org.apache.calcite" % "calcite-core" % "1.17.0"
libraryDependencies += "org.apache.calcite" % "calcite-linq4j" % "1.17.0"
libraryDependencies += "org.apache.parquet" % "parquet-common" % "1.9.0"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.7"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.7"
libraryDependencies += "ch.qos.logback" % "logback-core" % "1.2.3"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
libraryDependencies += "org.slf4j" %  "jul-to-slf4j" % "1.7.0"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"
libraryDependencies += "org.apache.parquet" % "parquet-common" % "1.10.0"
libraryDependencies += "org.apache.parquet" % "parquet-hadoop" % "1.10.0"
libraryDependencies += "org.apache.parquet" % "parquet-arrow" % "1.10.0"
libraryDependencies += "org.apache.arrow" % "arrow-vector" % "0.11.0"
libraryDependencies += "org.apache.arrow" % "arrow-memory" % "0.11.0"
libraryDependencies += "net.wrap-trap" % "parquet-to-arrow" % "0.3.0-SNAPSHOT"
libraryDependencies += "org.mockito" % "mockito-core" % "2.23.4" % "test"