package PlaygroundUtils

import java.io.{File, PrintWriter}
import java.nio.file.Paths
import java.text.SimpleDateFormat

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * Created by researchuser7 on 2020-03-09.
  */
object PlaygroundTools {
  val sourceDataPath = "file:///Users/researchuser7/Desktop/tpc-test/db_out/"
  val persistOutPath = "/Users/researchuser7/IdeaProjects/flink-playground-jobs/out/execution_plans" //TODO move to config file

  val HEAP_SIZE = "8GB"

  case class JobConfig(
                      jobType:String,
                      datasetSize:Long,
                      netRunTime: Option[Long] = None
                      )

  object JobConfig {
    implicit def long2Option(s: Long) = Some(s)
  }

  def persistExecutionPlan(execPlan:String, jobConfig:PlaygroundTools.JobConfig, outPath:String): Unit ={

    val timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new java.util.Date())

    //val reachExecPlan = execPlan.replaceFirst("\\{", "\\{\n\t"+ "\"netRunTime\":" + s"${jobConfig.netRunTime.getOrElse(-1)},")

    val reachExecPlan = "{\n" +
      "\t\"netRunTime\": " + s"${jobConfig.netRunTime.getOrElse(-1)},\n" +
      "\t\"executionPlan\": " + s"$execPlan\n" +
      "}"

    val outFileName = s"$timestamp-${jobConfig.jobType}.json"
    val outFilePath = Paths.get(outPath, outFileName)
    println(s"Storing execution plan in: $outFilePath")

    val pw = new PrintWriter(new File(outFilePath.toString))
    // pw.write(execPlan)

    pw.write(reachExecPlan)
    pw.close()
  }

  def saveExecutionPlan(jobID:String, execPlan:String, execTime:Option[Long], params: Map[String, String], env:ExecutionEnvironment): Unit = {
    import java.io.{File, PrintWriter};
    import java.nio.file.Paths;
    val outFilePath = Paths.get(params("execPlanOutPath"), StringContext("", ".json").s(jobID))
    val pw = new PrintWriter(new File(outFilePath.toString));

    val reachExecPlan = "{\n" +
      "\t\"dataPath\": \"" + s"${params("dataPath")}" + "\",\n" +
      "\t\"execPlanOutPath\": \"" + s"${params("execPlanOutPath")}" + "\",\n" +
      "\t\"execute\": \"" + s"${params("execute")}" + "\",\n" +
      "\t\"local\": \"" + s"${params("local")}" + "\",\n" +
      "\t\"heap_size\": \"" + s"${params("heap_size")}" + "\",\n" +
      "\t\"netRunTime\": " + s"${execTime.getOrElse(-1)},\n" +
      "\t\"environmentConfig\": \"" + s"${env.getJavaEnv.getConfiguration.toString}" + "\",\n" +
      "\t\"executionConfig\": \"" + s"${env.getConfig.toString}" + "\",\n" +
      "\t\"executionPlan\": " + s"$execPlan\n" +
      "}"
    pw.write(reachExecPlan)
    pw.close()
  }

}
