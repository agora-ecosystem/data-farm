package Generator

import Generator.DatasetJob.{AbstractDatasetOperatorManager, AbstractTableOperatorManager, JoinRelation}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration

import scala.util.Random

/**
  * Created by researchuser7 on 2020-03-17.
  */
case class AbstractOperator(
                             opId: String,
                             var tableOperatorManager: Option[AbstractTableOperatorManager] = None,
                             parents: Option[Array[AbstractOperator]] = None,
                             children: Option[Array[AbstractOperator]] = None
                           ) {

  // opId structure is <pact>_<id>
  val opPact: String = opId.split("_")(0)
  val nId: Int = opId.split("_")(1).toInt

  def getVariableName(): String = {
    this.opPact match {
      case AbstractOperator.DATA_SOURCE => s"source${this.nId}"
      case AbstractOperator.MAP => s"map${this.nId}"
      case AbstractOperator.FILTER => s"filter${this.nId}"
      case AbstractOperator.JOIN => s"join${this.nId}"
      case AbstractOperator.REDUCE => s"reduce${this.nId}"
      case AbstractOperator.GROUP_BY => s"group${this.nId}"
      case AbstractOperator.BULK_ITERATION => s"iteration${this.nId}"
      case AbstractOperator.PARTITION => s"partition${this.nId}" //TODO check if it return something
      case AbstractOperator.SORT_PARTITION => s"sort${this.nId}"
      case AbstractOperator.DATA_SINK => s"sink${this.nId}" //TODO check if it return something
      case _ => throw new Exception(s"Unknown pact ${this.opPact}")
    }
  }

  // New APIs
  def dataSourceCode(): String = {
    if (JobInfoRecorder.currentJobInfo.isDefined){
      JobInfoRecorder.currentJobInfo.get.record(
        this.nId,
        this.getVariableName(),
        this.opPact,
        Seq(this.tableOperatorManager.get.tableName),
        selectivity = Some(1.0)
      )
    }
    tableOperatorManager.get.dataSourceCode(this.getVariableName())
  }

  def filterCode(inVarName: String, seed:Int = 0): String = {
    val (filterCode, selectivity) = tableOperatorManager.get.filterCode(inVarName, this.getVariableName(), seed, seed)

    if (JobInfoRecorder.currentJobInfo.isDefined){
      JobInfoRecorder.currentJobInfo.get.record(
        this.nId,
        this.getVariableName(),
        this.opPact,
        Seq(this.tableOperatorManager.get.tableName),
        selectivity = Some(selectivity)
      )
    }
    filterCode
  }

  def mapCode(inVarName: String, complexity: Int = 0): String = {
    val c = tableOperatorManager.get.mapCode(inVarName, this.getVariableName(), complexity)
    if (JobInfoRecorder.currentJobInfo.isDefined){
      JobInfoRecorder.currentJobInfo.get.record(
        this.nId,
        this.getVariableName(),
        this.opPact,
        Seq(this.tableOperatorManager.get.tableName),
        selectivity = Some(1.0),
        complexity = Some(c._2)
      )
    }
    c._1
  }

  def reduceCode(inVarName: String, nextOp: Option[AbstractOperator] = None, seed:Int = 0): String = {
    val (c, outCard) = nextOp match {
      case None => (tableOperatorManager.get.reduceCode1(inVarName, this.getVariableName()), 1)
      case Some(op) => (tableOperatorManager.get.reduceCode2(inVarName, this.getVariableName(), nextOp.get.getVariableName(), seed), 1)
    }

    if (JobInfoRecorder.currentJobInfo.isDefined){
      JobInfoRecorder.currentJobInfo.get.record(
        this.nId,
        this.getVariableName(),
        this.opPact,
        Seq(this.tableOperatorManager.get.tableName),
        outCardinality = Some(outCard) //TODO set the correct outCardinality for the two different reduce cases
      )
    }
    c
  }

  def groupByCode(inVarName: String, nextOp: Option[AbstractOperator] = None, seed:Int = 0): String = {
    val (c, selectivity) = nextOp match {
      case Some(op) => tableOperatorManager.get.groupByCode2(inVarName, this.getVariableName(), nextOp.get.getVariableName(), seed)
      case None => tableOperatorManager.get.groupByCode1(inVarName, this.getVariableName(), seed)
    }

    if (JobInfoRecorder.currentJobInfo.isDefined){
      JobInfoRecorder.currentJobInfo.get.record(
        this.nId,
        this.getVariableName(),
        this.opPact,
        Seq(this.tableOperatorManager.get.tableName),
        outCardinality = Some(selectivity.toInt)
      )
    }
    c
  }

  def joinCode(lxVarName: String, rxVarName: String, joinRelation:JoinRelation): String = {
    if (JobInfoRecorder.currentJobInfo.isDefined){
      JobInfoRecorder.currentJobInfo.get.record(
        this.nId,
        this.getVariableName(),
        this.opPact,
        Seq(joinRelation.lx.tableName, joinRelation.rx.tableName)
      )
    }

    tableOperatorManager.get.joinCode(lxVarName, rxVarName, this.getVariableName(), joinRelation)
  }

  def partitionCode(inVarName: String): String = {
    if (JobInfoRecorder.currentJobInfo.isDefined){
      JobInfoRecorder.currentJobInfo.get.record(
        this.nId,
        this.getVariableName(),
        this.opPact,
        Seq(this.tableOperatorManager.get.tableName)
      )
    }
    tableOperatorManager.get.partitionCode(inVarName, this.getVariableName())
  }

  def sortPartitionCode(inVarName: String, seed:Int = 0): String = {
    if (JobInfoRecorder.currentJobInfo.isDefined){
      JobInfoRecorder.currentJobInfo.get.record(
        this.nId,
        this.getVariableName(),
        this.opPact,
        Seq(this.tableOperatorManager.get.tableName)
      )
    }
    tableOperatorManager.get.sortPartitionCode(inVarName, this.getVariableName(), seed)
  }

  def sinkCode(inVarName: String): String = {
    if (JobInfoRecorder.currentJobInfo.isDefined){
      JobInfoRecorder.currentJobInfo.get.record(
        this.nId,
        this.getVariableName(),
        this.opPact,
        Seq(this.tableOperatorManager.get.tableName)
      )
    }
    tableOperatorManager.get.sinkCode(inVarName, this.getVariableName())
  }





  //Old APIs
  private def getRandom(a: Array[String], seed: Int): String = {
    val random = Random
    random.setSeed(seed)
    a(random.nextInt(a.length))
  }


//  def dataSourceCode(sourceTable: Int): String = {
//    val sourceCode = sourceTable match {
//      case 0 => s"""
//                   |val ${this.getVariableName()} = env.readCsvFile[(Int, Int, Int, Int, Float, Float, Float, Float, String, String, java.sql.Date, java.sql.Date, java.sql.Date, String, String)](
//                   |  params("dataPath")+"${DatasetHelper.TPC_LINITEM_TABLE}", fieldDelimiter = "|"
//                   |)
//       """.stripMargin
//      case 1 => s"""
//                   |val ${this.getVariableName()} = env.readCsvFile[(Int, Int, String, Float, java.sql.Date, String, String, Int, String)](
//                   |  params("dataPath")+"${DatasetHelper.TPC_ORDERS_TABLE}", fieldDelimiter = "|"
//                   |)
//       """.stripMargin
//    }
//
//    sourceCode
//  }

//  def filterCode(inVar: String, sourceTable: Int): String = {
//
//    sourceTable match {
//      case 0 => s"""
//                   |val ${this.getVariableName()} = $inVar.filter(x=>x._11.getTime <= (dateFormatter.parseDateTime("1998-12-01").getMillis - 90L * 24L * 60L * 60L * 1000L))
//                """.stripMargin
//      case 1 => s"""
//                   |val ${this.getVariableName()} = $inVar.filter(x=>x._5.getTime <= (dateFormatter.parseDateTime("1998-12-01").getMillis - 90L * 24L * 60L * 60L * 1000L))
//                """.stripMargin
//    }
//
//  }

//  def reduceCode(inVar: String, nextOp: Option[AbstractOperator] = None): String =
//    nextOp match {
//      case Some(op) =>
//        s"""
//           |val ${this.getVariableName()} = $inVar.groupBy(0)
//           |val ${op.getVariableName()} = ${this.getVariableName()}.reduce((x1, x2) => x1)
//          """.stripMargin
//      case None =>
//        s"""
//           |val ${this.getVariableName()} = $inVar.reduce((x1, x2) => x1)
//          """.stripMargin
//    }

//  def joinCode(inVar1: String, inVar2: String, joinRelation:JoinRelation): String = {
//    // The Join return the rx tuple since the second input var, since there are only two tables, is always the the second popped from the stack (i.e. the linitem table)
//    s"""
//       |val ${this.getVariableName()} = $inVar1.join($inVar2).where(0).equalTo(0){
//       |      (lx, rx) => rx
//       |    }
//       """.stripMargin
//  }

  //  def sinkCode(inVar: String): String = {
  //    s"""
  //       |val ${this.getVariableName()} = $inVar.output(new DiscardingOutputFormat())
  //      """.stripMargin
  //  }

//  def groupByCode(inVar: String, nextOp: Option[AbstractOperator] = None): String = {
//    nextOp match {
//      case Some(op) =>
//        s"""
//           |val ${this.getVariableName()} = $inVar.groupBy(0)
//           |val ${op.getVariableName()} = ${this.getVariableName()}.first(1)
//          """.stripMargin
//      case None => //TODO check udf in reduceGroup, now the complexity is linear
//        s"""
//           |val ${this.getVariableName()} = $inVar.groupBy(0).reduceGroup {v => v.reduce((x1,x2) => x1)}
//          """.stripMargin
//    }
//  }

  def bulkIterationCode(inVar: String, iterType: Int = 1, nIteration: Int = 1000): String = {
    //Like KMeans and Gradient Descent iteration
    val iterUDF =
      s"""
         |import org.apache.flink.api.common.functions.RichMapFunction
         |import org.apache.flink.configuration.Configuration
         |
         |final class UDF$inVar extends RichMapFunction[(Int, Int, Int, Int, Float, Float, Float, Float, String, String, java.sql.Date, java.sql.Date, java.sql.Date, String, String), (Int, Int)] {
         |      private var bData: Traversable[(Int, Int)] = null
         |
                     |      override def open(parameters: Configuration) {
         |        bData = getRuntimeContext.getBroadcastVariable[(Int, Int)]("${inVar}bData").asScala
         |      }
         |
                     |      def map(x: (Int, Int, Int, Int, Float, Float, Float, Float, String, String, java.sql.Date, java.sql.Date, java.sql.Date, String, String)) = {
         |        var minDistance: Double = Double.MaxValue
         |        var counter: Int = 0
         |        for (d <- bData) {
         |          counter += 1
         |        }
         |        (x._1, x._2)
         |      }
         |
       |    }
       """
    iterType match {

      //Kmeans like
      case 0 => s"""
                   |    $iterUDF
                   |    val ${inVar}bData = $inVar.first(1000).map(x => (x._1, x._2))
                   |    val ${this.getVariableName()} = ${inVar}bData.iterate($nIteration) { d =>
                   |      val ${inVar}New = $inVar
                   |        .map(new UDF$inVar).withBroadcastSet(d, "${inVar}bData")
                   |        .map { x => x }
                   |        .groupBy(0)
                   |        .reduce { (x1, x2) => x1 }
                   |        .map { x => x }
                   |      ${inVar}New
                   |    }
                """.stripMargin

      //Gradient Descent like
      case 1 => s"""
                   |    $iterUDF
                   |    val ${inVar}bData = $inVar.first(1000).map(x => (x._1, x._2))
                   |    val ${this.getVariableName()} = ${inVar}bData.iterate($nIteration) { d =>
                   |      val ${inVar}New = ${inVar}
                   |        .map(new UDF${inVar}).withBroadcastSet(d, "${inVar}bData")
                   |        .reduce { (x1, x2) => x1}
                   |        .map {x => x}
                   |      ${inVar}New
                   |    }
                """.stripMargin
    }
  }

//  def partitionCode(inVar: String): String = {
//    s"""
//       |val ${this.getVariableName()} = $inVar.partitionByHash(0)
//      """.stripMargin
//
//  }

//  def sortPartitionCode(inVar: String): String = {
//    s"""
//       |val ${this.getVariableName()} = $inVar.sortPartition(1, Order.ASCENDING)
//      """.stripMargin
//
//  }

  override def equals(op: Any): Boolean = {
    op match {
      case op: AbstractOperator => opId == op.opId
      case _ => false
    }
  }

  override def hashCode(): Int = opId.hashCode()
}


object AbstractOperator {

  val DATA_SOURCE = "Data Source"
  val FILTER = "Filter"
  val MAP = "Map"
  val REDUCE = "Reduce"
  val GROUP_BY = "Group by"
  val JOIN = "Join"
  val BULK_ITERATION = "Bulk Iteration"
  val PARTITION = "Partition"
  val SORT_PARTITION = "Sort-Partition"
  val DATA_SINK = "Data Sink"


  val OPERATORS_MAPPING: Map[String, Array[String]] = Map(
    DATA_SOURCE -> Array("Data Source"),
    FILTER -> Array("Filter"),
    MAP -> Array("Map", "FlatMap", "CoMap", "CoFlatMap", "MapPartition"),
    REDUCE -> Array("Reduce"),
    GROUP_BY -> Array("GroupReduce", "CoGroup", "GroupCombine"),
    JOIN -> Array("Join", "Outer Join"),
    BULK_ITERATION -> Array("Bulk Iteration", "Workset Iteration"),
    PARTITION -> Array("Partition"),
    SORT_PARTITION -> Array("Sort-Partition"),
    DATA_SINK -> Array("Data Sink")
  )

  val OPERATOR_GROUPS: Set[String] = OPERATORS_MAPPING.keys.toSet
  val OPERATORS: Set[String] = OPERATORS_MAPPING.values.flatten.toSet


}
