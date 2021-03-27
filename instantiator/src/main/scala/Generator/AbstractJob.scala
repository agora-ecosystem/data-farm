package Generator



import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox
import scala.reflect.runtime.universe._
import java.io.File
import java.nio.file.Paths
/**
  * Created by researchuser7 on 2020-03-17.
  */

case class AbstractJob(jobBody:String, jobName:String, saveExecutionPlan:Boolean=false){

  var filledBody:String = fillBody()
  var filledSBT:String = fillSBT()

  def fillBody(): String ={
//    if (saveExecutionPlan && planOutPath.isEmpty){
//      println("WARNING - Request to save execution plan but no plan out path has been provided. Execution plan will not be saved!")
//    }
    AbstractJob.JOB_TEMPLATE
      .replace("//#job_name#//", this.jobName)
      .replace("//#body#//", this.jobBody)
      .replace("//#save_plan#//", if (saveExecutionPlan) AbstractJob.saveExecPlanCode() else "")
  }

  def fillSBT(): String = {
    AbstractJob.SBT_TEMPLATE
      .replace("//#job_name#//", this.jobName)
  }

  def createSBTProject(projectPath:String): Unit ={

    val tb = currentMirror.mkToolBox()
    val parsedTree = tb.parse(filledBody)

    val finalProjectPath = Paths.get(projectPath, jobName).toString
    println(s"Saving SBT project to $finalProjectPath")

    val srcDirRes = new File(s"$finalProjectPath/src/main/scala").mkdirs()
    val projectDirRes = new File(s"$finalProjectPath/project").mkdirs()

    //val res = s"mkdir -p $projectPath/src/{main,test}/{java,resources,scala}" !
    //val res1 = s"mkdir $projectPath/lib $projectPath/project $projectPath/target" !

    val jobWriter = new java.io.PrintWriter(s"$finalProjectPath/src/main/scala/$jobName.scala")
    val buildWriter = new java.io.PrintWriter(s"$finalProjectPath/build.sbt")
    val pluginsWriter = new java.io.PrintWriter(s"$finalProjectPath/project/plugins.sbt")
    try {
      jobWriter.write(showCode(parsedTree))
      buildWriter.write(filledSBT)
      pluginsWriter.write("""addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.10")""")
    }
    finally {
      jobWriter.close()
      buildWriter.close()
      pluginsWriter.close()
    }
  }

}

object AbstractJob {

  val SBT_TEMPLATE: String =
    """
       |name := "//#job_name#//"
       |
       |version := "0.1"
       |
       |scalaVersion := "2.11.12"
       |
       |val flinkVersion = "1.10.0"
       |val flinkConf = "provided" // "compile" | "provided"
       |
       |val flinkDependencies = Seq(
       |  "org.apache.flink" %% "flink-scala" % flinkVersion % flinkConf,
       |  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % flinkConf,
       |  "org.apache.flink" % "flink-hadoop-fs" % flinkVersion % "compile"
       |)
       |
       |val utilsDependencies = Seq(
       |  "joda-time" % "joda-time" % "2.8.1"
       |)
       |
       |libraryDependencies ++= flinkDependencies
       |libraryDependencies ++= utilsDependencies
       |
       |assemblyJarName in assembly := "//#job_name#//.jar"
     """.stripMargin

  val JOB_TEMPLATE: String =
    """
       |object //#job_name#// {
       |
       |  import org.apache.flink.api.scala._
       |  import org.apache.flink.api.java.io.DiscardingOutputFormat
       |  import org.apache.flink.api.scala.ExecutionEnvironment
       |  import org.apache.flink.api.common.operators.Order
       |  import org.apache.flink.configuration.Configuration
       |
       |  import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
       |
       |  import scala.collection.JavaConverters._
       |
       |  val dateFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
       |
       |  def run(params: Map[String, String]): Unit = {
       |    val env = if (params.getOrElse("local", "false") == "true") {
       |      val conf = new Configuration()
       |      conf.setString("taskmanager.heap.size", params.getOrElse("heap_size", "4G"))
       |      val lEnv = ExecutionEnvironment.createLocalEnvironment(conf)
       |      val executionConfig = lEnv.getConfig
       |      executionConfig.setParallelism(params.getOrElse("parallelism", "4").toInt)
       |      lEnv
       |    } else {
       |      ExecutionEnvironment.getExecutionEnvironment
       |    }
       |
       |    //#body#//
       |
       |    val execPlan = env.getExecutionPlan()
       |    val execTime = if (params.getOrElse("execute", "false") == "true"){
       |      val execResult = env.execute
       |      println(StringContext("Runtime execution: ", " ms").s(execResult.getNetRuntime))
       |      Some(execResult.getNetRuntime)
       |    } else {
       |      None
       |    }
       |
       |    //#save_plan#//
       |  }
       |
       |  def main(args: Array[String]): Unit = {
       |    val params = if (args.length==6){
       |      Map(
       |        "dataPath" -> args(0),
       |        "execPlanOutPath" -> args(1),
       |        "execute" -> (args(2) == "exec").toString,
       |        "local" -> (args(3) == "local").toString,
       |        "heap_size" -> args(4),
       |        "parallelism"->args(5)
       |      )
       |    }
       |    else {
       |      throw new Exception("Expected arguments: <data_path> <exec_plan_out_path> <exec|noexec> <local|nolocal> <heap_size> <parallelism>")
       |    }
       |    run(params)
       |  }
       |
       |  def saveExecutionPlan(execPlan:String, execTime:Option[Long], params: Map[String, String], env:ExecutionEnvironment): Unit = {
       |    import java.io.{File, PrintWriter};
       |    import java.nio.file.Paths;
       |    val outFilePath = Paths.get(params("execPlanOutPath"), StringContext("", ".json").s(this.getClass.getName))
       |    val pw = new PrintWriter(new File(outFilePath.toString));
       |
       |    val reachExecPlan = "{\n" +
       |      "\t\"dataPath\": \"" + s"${params("dataPath")}" + "\",\n" +
       |      "\t\"execPlanOutPath\": \"" + s"${params("execPlanOutPath")}" + "\",\n" +
       |      "\t\"execute\": \"" + s"${params("execute")}" + "\",\n" +
       |      "\t\"local\": \"" + s"${params("local")}" + "\",\n" +
       |      "\t\"heap_size\": \"" + s"${params("heap_size")}" + "\",\n" +
       |      "\t\"netRunTime\": " + s"${execTime.getOrElse(-1)},\n" +
       |      "\t\"environmentConfig\": \"" + s"${env.getJavaEnv.getConfiguration.toString}" + "\",\n" +
       |      "\t\"executionConfig\": \"" + s"${env.getConfig.toString}" + "\",\n" +
       |      "\t\"executionPlan\": " + s"$execPlan\n" +
       |      "}"
       |    pw.write(reachExecPlan)
       |    pw.close()
       |  }
       |
       |}
     """.stripMargin

  def saveExecPlanCode() = s"""saveExecutionPlan(execPlan, execTime, params, env)"""




}
