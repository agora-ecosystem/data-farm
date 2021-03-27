package Generator.DatasetJob

import Generator.DatasetJob.utils.getElementBySeed


/**
  * Created by researchuser7 on 2020-04-21.
  */
trait AbstractTableOperatorManager {

  val tableName: String

  //val tableCardinality: Int

  val typeSchema: String

  val fields: Map[String, String]

  val joinFieldTable: Map[String, Map[String, String]]

  val filterFieldValue: Map[String, Map[String, Seq[String]]]

  val groupFields: Map[String, String]

  def joinTables: Seq[String] = joinFieldTable.mapValues(v => v.keys).flatMap(_._2).toSet.toSeq

  def joinFields: Seq[String] = joinFieldTable.keys.toSeq

  def filterFields: Seq[String] = filterFieldValue.keys.toSeq


  //Operators to implement

  def filterCode(inVarName: String, outVarName: String, fieldSeed: Int = 0, valueSeed: Int = 0): (String, Double)

  //Operator builders

  def buildDataSourceCode(schema: String, tableName: String, outVarName: String, delimiter:String = "|"): String = {
    s"""
       |val $outVarName = env.readCsvFile[$schema](
       |  params("dataPath")+"$tableName", fieldDelimiter = "$delimiter"
       |)
       """.stripMargin
  }

  def buildFilterCode(inVarName: String, outVarName: String, filterField: String, filterValue: String, filterOp: String = "<="): String = {
    s"""
       |val $outVarName = $inVarName.filter(x=>x._$filterField $filterOp $filterValue)
      """.stripMargin
  }

  def buildReduceCode2(inVarName: String, outVarName1: String, outVarName2: String, groupByField: String): String = {
    s"""
       |val $outVarName1 = $inVarName.groupBy($groupByField)
       |val $outVarName2 = $outVarName1.reduce((x1, x2) => x1)
      """.stripMargin
  }

  def buildGroupByCode1(inVarName: String, outVarName: String, groupByField: String): String = {
    s"""
       |val $outVarName = $inVarName.groupBy($groupByField).reduceGroup{v => v.reduce((x1,x2) => x1)}
          """.stripMargin
  }

  def buildGroupByCode2(inVarName: String, outVarName1: String, outVarName2: String, groupByField: String): String = {
    //TODO check aggregation function complexity
    s"""
       |val $outVarName1 = $inVarName.groupBy($groupByField)
       |val $outVarName2 = $outVarName1.reduce((lx, rx) => rx)
          """.stripMargin
  }

    def buildSortPartitionCode(inVarName: String, outVarName: String, sortField: String, order: String = "ASCENDING"): String = {
    s"""
       |val $outVarName = $inVarName.sortPartition($sortField, Order.$order)
      """.stripMargin
  }

  def buildJoinCode(lxVarName: String, rxVarName: String, outVarName: String, joinRelation: JoinRelation): String = {
    val lxJFieldId = joinRelation.lx.fields(joinRelation.field).toInt - 1
    val rxJFieldId = joinRelation.rx.fields(joinRelation.field).toInt - 1

    s"""
       |val $outVarName = $lxVarName.join($rxVarName).where($lxJFieldId).equalTo($rxJFieldId){
       |      (lx, rx) => rx
       |    }
       """.stripMargin
  }

  //Operators already implemented for every table

  def reduceCode1(inVarName: String, outVarName: String): String = {
    s"""
       |val $outVarName = $inVarName.reduce((x1, x2) => x1)
      """.stripMargin
  }


  def mapCode(inVarName: String, outVarName: String, seed: Int = 0): (String, Int) = {
    val complexity = seed % 3
    val c = complexity match {
      case 0 => s"""
                   |val $outVarName = $inVarName.map(x=>x)
                """.stripMargin
      case 1 => s"""
                   |val $outVarName = $inVarName.map(x=> {var count = 0; for (v <- x.productIterator) count+=1; x})
                """.stripMargin
      case 2 => s"""
                   |val $outVarName = $inVarName.map(x=> {var count = 0; for (v1 <- x.productIterator; v2 <- x.productIterator) count+=1; x})
       """.stripMargin
    }
    (c, complexity)
  }

  def joinCode(lxVarName: String, rxVarName: String, outVarName: String, joinRelation: JoinRelation): String = {
    buildJoinCode(lxVarName: String, rxVarName: String, outVarName: String, joinRelation: JoinRelation)
  }

  def partitionCode(inVarName: String, outVarName: String): String = {
    s"""
       |val $outVarName = $inVarName.partitionByHash(0)
      """.stripMargin

  }

  def dataSourceCode(outVarName: String): String = buildDataSourceCode(typeSchema, tableName, outVarName)

  def groupByCode1(inVarName: String, outVarName: String, seed: Int): (String, Double) = {
    val gField = getElementBySeed(groupFields.keys.toSeq, seed).toString
    val gFieldId = fields(gField).toInt-1
    val code = buildGroupByCode1(inVarName, outVarName, gFieldId.toString)
    (code, groupFields(gField).toDouble)
  }

  def groupByCode2(inVarName: String, outVarName1: String, outVarName2: String, seed: Int): (String, Double) = {
    val gField = getElementBySeed(groupFields.keys.toSeq, seed).toString
    val gFieldId = fields(gField).toInt-1
    val code = buildGroupByCode2(inVarName, outVarName1, outVarName2, gFieldId.toString)
    (code, groupFields(gField).toDouble)
  }

  def reduceCode2(inVarName: String, outVarName1: String, outVarName2: String, seed: Int): String = {
    val rField = getElementBySeed(groupFields.keys.toSeq, seed).toString
    val rFieldId = fields(rField).toInt-1
    val code = buildReduceCode2(inVarName, outVarName1, outVarName2, rFieldId.toString)
    code
  }

  def sortPartitionCode(inVarName: String, outVarName: String, seed: Int): String = {
    val sField = getElementBySeed(fields.keys.toSeq, seed).toString
    val sFieldId = fields(sField).toInt - 1
    buildSortPartitionCode(inVarName, outVarName, sFieldId.toString)
  }

  def sinkCode(inVarName: String, outVarName: String): String = {
    s"""
       |val $outVarName = $inVarName.output(new DiscardingOutputFormat())
      """.stripMargin
  }


  override def equals(obj: Any): Boolean =
    obj match {
      case t: AbstractTableOperatorManager => tableName == t.tableName
      case _ => false
    }

  override def toString: String = tableName


}
