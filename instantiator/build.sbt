name := "instantiator"

version := "0.1"

scalaVersion := "2.11.12"

val flinkVersion = "1.10.0"
val flinkConf = "compile" // "compile" | "provided"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % flinkConf,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % flinkConf,
  "org.apache.flink" %% "flink-gelly" % flinkVersion % flinkConf,
  "org.apache.flink" %% "flink-table-api-scala-bridge" % flinkVersion % flinkConf,
  "org.apache.flink" %% "flink-table-planner" % flinkVersion % flinkConf,
  "org.apache.flink" %% "flink-connector-filesystem" % flinkVersion % flinkConf,
  "org.apache.flink" % "flink-hadoop-fs" % flinkVersion % "compile"
)

val flinkWalktrhoughtDependencies = Seq(
  "org.apache.flink" %% "flink-walkthrough-common" % flinkVersion
)

val loggingDependencies = Seq(
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.13.1" % "runtime",
  "org.apache.logging.log4j" % "log4j-core" % "2.13.1" % "runtime",
  "org.apache.logging.log4j" % "log4j-api" % "2.13.1" % "runtime"
)


val utilsDependencies = Seq(
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.10.3" % flinkConf,
  "org.jgrapht" % "jgrapht-core" % "1.4.0",
  "joda-time" % "joda-time" % "2.8.1",
  "org.jblas" % "jblas" % "1.2.3",
  //dependecy for flink.ml
  "org.scalanlp" %% "breeze" % "0.13" 
)

libraryDependencies ++= flinkDependencies
libraryDependencies ++= utilsDependencies