package Generator.DatasetJob.imdb_kaggle.table

import Generator.DatasetJob.AbstractTableOperatorManager
import Generator.DatasetJob.utils.getElementBySeed

/**
  * Created by researchuser7 on 2020-06-24.
  */
case class TitleBasicsOperatorManager() extends AbstractTableOperatorManager {
  override val tableName: String = "title.basics"
  override val typeSchema: String = "(String, String, String, String, String, Int, Int, Int, String)"
  override val fields: Map[String, String] = Map(
    "titleId" -> "1",
    "titleType" -> "2",
    "primaryTitle" -> "3",
    "originalTitle" -> "4",
    "isAdult" -> "5",
    "startYear" -> "6",
    "endYear" -> "7",
    "runtimeMinutes" -> "8",
    "genres" -> "9"
  )
  override val joinFieldTable: Map[String, Map[String, String]] = Map("titleId" -> Map("title.principals" -> "titleId", "title.akas" -> "titleId", "title.ratings" -> "titleId"))
  override val filterFieldValue: Map[String, Map[String, Seq[String]]] = Map(
    "titleType"-> Map("selectivity"-> Seq("0.08476158788090499", "0.11250643123537413", "0.7026607729811453", "0.0044910768831961205", "0.019034718001689706", "0.027598159817088158", "0.0018286442284058677", "0.004131322862636715", "0.03910681106354258", "0.0038804750460164276"), "values"-> Seq("movie", "short", "tvEpisode", "tvMiniSeries", "tvMovie", "tvSeries", "tvShort", "tvSpecial", "video", "videoGame")),
    "isAdult"-> Map("selectivity"-> Seq("0.97", "0.03"), "values"-> Seq("0.0", "1.0")),
    "startYear"-> Map("selectivity"-> Seq("0.25", "0.5", "0.75"), "values"-> Seq("1992.0", "2008.0", "2015.0")))

  override val groupFields: Map[String, String] = Map("isAdult" -> "5", "titleType" -> "10")


  override def dataSourceCode(outVarName: String): String = buildDataSourceCode(typeSchema, tableName + ".csv", outVarName, delimiter = ";")

  def filterCode(inVarName: String, outVarName: String, fieldSeed: Int = 0, valueSeed: Int = 0): (String, Double) = {
    val field = filterFields(fieldSeed % filterFields.size)

    var filterField = ""
    var filterValue = ""
    var filterOp = ""

    field match {
      case "titleType" =>
        filterField = fields(field)
        filterValue = s""" "${getElementBySeed(filterFieldValue(field)("values"), valueSeed).toString}" """
        filterOp = "=="

      case "isAdult" =>
        filterField = fields(field)
        filterValue = s""" "${getElementBySeed(filterFieldValue(field)("values"), valueSeed).toString}" """
        filterOp = "=="

      case "startYear" =>
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
}
