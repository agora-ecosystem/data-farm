package Generator

import java.io.File
import java.nio.file.{Files, Paths}

import Generator.DatasetJob.{AbstractDatasetOperatorManager, AbstractTableOperatorManager, JoinRelation}

import scala.collection.mutable
import scala.reflect.runtime.universe._
import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox
import scala.util.Random
import util.control.Breaks._

/**
  * Created by researchuser7 on 2020-03-16.
  */
case class JobGenerator(abstractPlan: AbstractExecutionPlan, datasetOperatorManager: AbstractDatasetOperatorManager, seed: Int) {

  def this(abstractPlan: AbstractExecutionPlan, datasetOperatorManager: AbstractDatasetOperatorManager) = this(abstractPlan, datasetOperatorManager, 0)

  val random: Random.type = Random

  random.setSeed(seed)

  val nJoins: Int = abstractPlan.vertexSet().filter(_.opPact == AbstractOperator.JOIN).toSeq.length

  val (tableSequence, joinSequence) = {
    val (tS, jS) = datasetOperatorManager.getSourceTableSequence(nJoins, this.seed)

    println(s"Table sequence: $tS")
    println()
    println("Join sequence:")
    jS.foreach(j => println(s"> $j"))
    println()

    (tS, jS)
  }

  if (JobInfoRecorder.currentJobInfo.isDefined) {
    JobInfoRecorder.currentJobInfo.get.tableSequence = Some(tableSequence)
    JobInfoRecorder.currentJobInfo.get.joinRelationsSequence = Some(joinSequence)
  }

  private val tableQueue: mutable.Queue[AbstractTableOperatorManager] = mutable.Queue()
  tableSequence.foreach(v => tableQueue.enqueue(v))

  private val joinQueue: mutable.Queue[JoinRelation] = mutable.Queue()
  joinSequence.foreach(v => joinQueue.enqueue(v))

  private var prevVars: mutable.Stack[String] = mutable.Stack()
  private var mapCount: Int = 0

  def getOpCode(op: AbstractOperator, nextOp: Option[AbstractOperator] = None, secondOp: Option[AbstractOperator] = None): String = {

    val outVar = if (nextOp.isDefined && secondOp.isEmpty)
      nextOp.get.getVariableName()
    else if (nextOp.isEmpty && secondOp.isDefined)
      op.getVariableName()
    else if (nextOp.isDefined && secondOp.isDefined)
      throw new Exception("Impossible to manage nextOp and secondOp together")
    else
      op.getVariableName()

    val genCode = op.opPact match {

      case AbstractOperator.DATA_SOURCE => {

        op.dataSourceCode()
      }

      case AbstractOperator.MAP => {
        val c = op.mapCode(prevVars.pop(), complexity = mapCount % 3)
        mapCount += 1
        c
      }

      case AbstractOperator.FILTER => {
        op.filterCode(prevVars.pop(), random.nextInt().abs)
      }

      case AbstractOperator.REDUCE => op.reduceCode(prevVars.pop(), nextOp, random.nextInt().abs)

      case AbstractOperator.JOIN => {
        val rxInVar = prevVars.pop()
        val lxInVar = prevVars.pop()
        val jRelation = joinQueue.dequeue()

        val c = op.joinCode(lxInVar, rxInVar, jRelation)
        c
      }

      case AbstractOperator.GROUP_BY => op.groupByCode(prevVars.pop(), nextOp, random.nextInt().abs)

      //TODO update bulk iteration before uncomment
      //case AbstractOperator.BULK_ITERATION => op.bulkIterationCode(prevVars.pop())

      case AbstractOperator.PARTITION => op.partitionCode(prevVars.pop())

      case AbstractOperator.SORT_PARTITION => op.sortPartitionCode(prevVars.pop(), random.nextInt().abs)

      case AbstractOperator.DATA_SINK => op.sinkCode(prevVars.pop())

      case _ => throw new Exception(s"Unknown operator with pact: ${op.opPact}")

    }

    this.prevVars.push(outVar)

    println(s"|-> Queue status -> ${this.prevVars}")

    genCode
  }

  private var lastOperator: Option[AbstractOperator] = None

  private val colored: mutable.Set[String] = mutable.Set()
  //private var prevOpId = "Start"

  def colorEdge(opId1: String, opId2: String) = colored.add(s"$opId1;$opId2")

  def checkColored(opId1: String, opId2: String) = colored.contains(s"$opId1;$opId2")

  def _generate(abstractPlan: AbstractExecutionPlan, entryOpId: String): Seq[String] = {

    var prevOpId: String = "Start"
    var skipNext: Boolean = false

    var tableOpManager = tableQueue.dequeue() //Get tableManager for the branch
    val branchOperators = abstractPlan.depthVisitFrom(entryOpId)

    val operatorCodes = for (i <- branchOperators.indices) yield {
      var opCode = ""
      val op = branchOperators(i)
      op.tableOperatorManager = Some(tableOpManager) //Assign the tableManager to the operator


      // color current edge
      colorEdge(prevOpId, op.opId)
      println(s"|Coloring: $prevOpId - ${op.opId} ${if (skipNext) " - (skip)" else ""} -> table: ${op.tableOperatorManager.get}")

      breakable {
        if (skipNext) {
          skipNext = false
          break
        }
        //Manage Join operator
        if (op.opPact == AbstractOperator.JOIN) {

          // get join sources
          val jParents = abstractPlan.parents(op.opId)

          // go back only to not colored branch
          val sourcesOfJ = abstractPlan.sourcesOf(op.opId)
          val sourceNotColored = sourcesOfJ.find(s => !checkColored("Start", s.opId)).get

          // get subgraph
          val subAbsPlan = abstractPlan.getBranchFromTo(sourceNotColored.opId, op.opId)
          subAbsPlan.execPlan.removeVertex(op)

          // generate code for the new branch
          println("--- New branch --->")
          println("Sub plan:", subAbsPlan.vertexSet().mkString(", "))
          val opBeforeJoinCodes = _generate(subAbsPlan, sourceNotColored.opId)
          println("<--- Back ---")

          val rxOperator = lastOperator.get //lastOperator can not be empty here since join is never a starting op
          tableOpManager = rxOperator.tableOperatorManager.get //update the table manager for the branch

          val opJoinCode = getOpCode(op: AbstractOperator) //secondOp=Some(rxOperator)

          opCode = (opBeforeJoinCodes :+ opJoinCode).mkString("\n")
        }

        // TODO unify the code for the operator merging since they do the same thing
        //Manage two group by in sequence
        else if (op.opPact == AbstractOperator.GROUP_BY) {
          opCode = if (
            ((i + 1 >= 0) && (i + 1 < branchOperators.length)) &&
              (branchOperators(i + 1).opPact == AbstractOperator.GROUP_BY)
          ) {
            skipNext = true
            val nextOp = branchOperators(i + 1)
            nextOp.tableOperatorManager = Some(tableOpManager)
            getOpCode(op, nextOp=Some(nextOp))
          } else {
            getOpCode(op)
          }
        }
        //Manage two reduce in sequence
        else if (op.opPact == AbstractOperator.REDUCE) {
          opCode = if (
            ((i + 1 >= 0) && (i + 1 < branchOperators.length)) &&
              (branchOperators(i + 1).opPact == AbstractOperator.REDUCE)
          ) {
            skipNext = true
            val nextOp = branchOperators(i + 1)
            nextOp.tableOperatorManager = Some(tableOpManager)
            getOpCode(op, nextOp=Some(nextOp))
          } else {
            getOpCode(op)
          }
        }
        //standard case
        else {
          opCode = getOpCode(op)
        }
      }

      //update
      prevOpId = op.opId
      lastOperator = Some(op)

      //yield generated code
      opCode
    }
    operatorCodes.filter(_.nonEmpty)
  }

  def generate(entryOpId: String = "Data Source_0", jobName: String, saveExecutionPlan: Boolean = false): AbstractJob = {
    //println(abstractPlan.vertexSet().map(_.opId).mkString(", "))

    val operatorCodes = _generate(abstractPlan: AbstractExecutionPlan, entryOpId: String)
    //println(operatorCodes)

    AbstractJob(operatorCodes.mkString("\n"), jobName, saveExecutionPlan = saveExecutionPlan)
  }
}

object JobGenerator {
  
  def parseArgs(args: Array[String]) = {
    val params = if (args.length >= 5)
      Map(
        "nJobs" -> args(0),
        "nVersions" -> args(1),
        "dataManager" -> args(2),
        "abstractPlansSource" -> args(3),
        "genJobsDest" -> args(4),
        "jobSeed" -> args.lift(5).getOrElse("-1")

      )
    else
      throw new Exception("Expected arguments: <nJobs> <nVersions> <dataManager> <abstractPlansSource> <genJobsDest> [jobSeed]")
    params
  }


  def main(args: Array[String]): Unit = {

    val params = parseArgs(args)

    // Check if output folder exists
    if (Files.notExists(Paths.get(params("genJobsDest")))) {
      throw new Exception(s"Destination folder for genJobsDest '${params("genJobsDest")}' does not exists.")
    }

    // Qausiquotes toolbox
    val tb = currentMirror.mkToolBox()

    val abstractPlanPaths = JsonUtil.getListOfJsonFilePaths(params("abstractPlansSource"))

    //Generate Jobs
    //for (i <- 0 until params("nJobs").toInt) {
    for (absPlanPath <- abstractPlanPaths.take(params("nJobs").toInt)){

      val i = Paths.get(absPlanPath).getFileName.toString.replace(".json", "").split("_").last.toInt

      val abstractPlan = AbstractExecutionPlan.parseExecPlan(Paths.get(absPlanPath).toString)
      val tpchDatasetOperatorManager = AbstractDatasetOperatorManager(params("dataManager"))

      for (j <- 0 until params("nVersions").toInt) {

        println("\n---------------------------------------")
        println(s"---- GENERATING JOB $i v$j --------------\n")
        val jobId = s"Job${i}v$j"
        JobInfoRecorder.openJobRecorder(jobId)

        val jSeed = if (params("jobSeed").toInt > -1){
          println(s"WARN - Setting job seed to ${params("jobSeed").toInt}")
          params("jobSeed").toInt
        } else i

        val jGenerator = new JobGenerator(abstractPlan, tpchDatasetOperatorManager, seed = 10 * jSeed + j)
        val generatedJob = jGenerator.generate(
          abstractPlan.getVertexFromId("Data Source_0").opId,
          jobId,
          saveExecutionPlan = true
        )

        println(JobInfoRecorder.currentJobInfo.get)
        println(JobInfoRecorder.currentJobInfo.get.toJson)
        JobInfoRecorder.closeJobRecorder()

        generatedJob.createSBTProject(params("genJobsDest"))
      }


    }

    //println(JobInfoRecorder.toJson)
    JobInfoRecorder.persist(params("genJobsDest"))
  }


}
