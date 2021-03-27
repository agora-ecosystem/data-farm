package Generator.DatasetJob.imdb_kaggle.table

import Generator.DatasetJob.AbstractTableOperatorManager
import Generator.DatasetJob.utils.getElementBySeed

/**
  * Created by researchuser7 on 2020-06-24.
  */
case class TitleRatingsOperatorManager() extends AbstractTableOperatorManager {
  override val tableName: String = "title.ratings"
  override val typeSchema: String = "(String, Float, Int)"
  override val fields: Map[String, String] = Map(
    "titleId" -> "1",
    "averageRating" -> "2",
    "numVotes" -> "3"
  )
  override val joinFieldTable: Map[String, Map[String, String]] = Map("titleId" -> Map("title.principals" -> "titleId", "title.akas" -> "titleId", "title.basics" -> "titleId"))

  override val filterFieldValue: Map[String, Map[String, Seq[String]]] = Map(
    "averageRating" -> Map("selectivity" -> Seq("0.25", "0.5", "0.75"), "values" -> Seq("6.1", "7.1", "7.9")),
    "numVotes" -> Map("selectivity" -> Seq("0.25", "0.5", "0.75"), "values" -> Seq("9.0", "20.0", "76.0")))

  override val groupFields: Map[String, String] = Map("averageRating" -> "10")

  override def dataSourceCode(outVarName: String): String = buildDataSourceCode(typeSchema, tableName + ".csv", outVarName, delimiter = ";")

  def filterCode(inVarName: String, outVarName: String, fieldSeed: Int = 0, valueSeed: Int = 0): (String, Double) = {
    val field = filterFields(fieldSeed % filterFields.size)

    var filterField = ""
    var filterValue = ""
    var filterOp = ""

    field match {
      case "averageRating" =>
        filterField = fields(field)
        filterValue = getElementBySeed(filterFieldValue(field)("values"), valueSeed).toString
        filterOp = "<="

      case "numVotes" =>
        filterField = fields(field)
        filterValue = getElementBySeed(filterFieldValue(field)("values"), valueSeed).toString
        filterOp = "<="

      case _ => throw new Exception(s"Can not find field with name '$field' in filter operator generation.")
    }

    (
      buildFilterCode(inVarName, outVarName, filterField, filterValue, filterOp),
      getElementBySeed(filterFieldValue(field)("selectivity"), valueSeed).toString.toDouble
    )

  }


  def buildGroupByCodeCustom1(inVarName: String, outVarName: String, groupByField: String): String = {
    s"""
       |val $outVarName = $inVarName.map(x => x.copy(_2=x._2.toInt)).groupBy(1).reduceGroup{v => v.reduce((x1,x2) => x1)}
          """.stripMargin
  }

  def buildGroupByCodeCustom2(inVarName: String, outVarName1: String, outVarName2: String, groupByField: String): String = {
    //TODO check aggregation function complexity
    s"""
       |val $outVarName1 = $inVarName.map(x => x.copy(_2=x._2.toInt)).groupBy($groupByField)
       |val $outVarName2 = $outVarName1.reduce((lx, rx) => rx)
          """.stripMargin
  }

  override def groupByCode1(inVarName: String, outVarName: String, seed: Int): (String, Double) = {
    val gField = getElementBySeed(groupFields.keys.toSeq, seed).toString

    gField match {
      case "averageRating" =>
        val gFieldId = fields(gField).toInt-1
        val code = buildGroupByCodeCustom1(inVarName, outVarName, gFieldId.toString)
        (code, groupFields(gField).toDouble)
      case _ => throw new Exception(s"Can not find field with name '$gField' in groupBy operator generation.")
    }

  }

  override def groupByCode2(inVarName: String, outVarName1: String, outVarName2: String, seed: Int): (String, Double) = {
    val gField = getElementBySeed(groupFields.keys.toSeq, seed).toString
    gField match {
      case "averageRating" =>
        val gFieldId = fields(gField).toInt-1
        val code = buildGroupByCode2(inVarName, outVarName1, outVarName2, gFieldId.toString)
        (code, groupFields(gField).toDouble)
      case _ => throw new Exception(s"Can not find field with name '$gField' in groupBy operator generation.")
    }
  }
}
