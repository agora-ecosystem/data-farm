name := "//#job_name#//"

version := "0.1"

scalaVersion := "2.11.12"

val flinkVersion = "1.10.0"
val flinkConf = "provided" // "compile" | "provided"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % flinkConf,
  "org.apache.flink" % "flink-hadoop-fs" % flinkVersion % "compile"
)

val utilsDependencies = Seq(
  "joda-time" % "joda-time" % "2.8.1"
)

libraryDependencies ++= flinkDependencies
libraryDependencies ++= utilsDependencies

assemblyJarName in assembly := "//#job_name#//.jar"