package Generator.DatasetJob.imdb_kaggle.table

import Generator.DatasetJob.AbstractTableOperatorManager
import Generator.DatasetJob.utils.getElementBySeed

/**
  * Created by researchuser7 on 2020-06-24.
  */
case class TitlePrincipalsOperatorManager() extends AbstractTableOperatorManager {
  override val tableName: String = "title.principals"
  override val typeSchema: String = "(String, Int, String, String, String, String)"
  override val fields: Map[String, String] = Map(
    "titleId" -> "1",
    "ordering" -> "2",
    "nconst" -> "3",
    "category" -> "4",
    "job" -> "5",
    "characters" -> "6"
  )
  override val joinFieldTable: Map[String, Map[String, String]] = Map("titleId"-> Map("title.akas"-> "titleId", "title.ratings"-> "titleId", "title.basics"-> "titleId"), "nconst"-> Map("name.basics"-> "nconst"))

  override val filterFieldValue: Map[String, Map[String, Seq[String]]] = Map(
    "category"-> Map("selectivity"-> Seq("0.2328751487957272", "0.17343589964455602", "0.005732238266918548", "5.868540742138621e-05", "0.03566448100510623", "0.03599766178925725", "0.11461268288641464", "0.032844622520774414", "0.06027980391293036", "0.007837734793684902", "0.16869169678745888", "0.13196934418975015"), "values"-> Seq("actor", "actress", "archive_footage", "archive_sound", "cinematographer", "composer", "director", "editor", "producer", "production_designer", "self", "writer")))

  override val groupFields: Map[String, String] = Map("category"-> "12", "nconst"-> "3873199", "titleId"-> "5710740")

  override def dataSourceCode(outVarName: String): String = buildDataSourceCode(typeSchema, tableName + ".csv", outVarName, delimiter = ";")

  def filterCode(inVarName: String, outVarName: String, fieldSeed: Int = 0, valueSeed: Int = 0): (String, Double) = {
    val field = filterFields(fieldSeed % filterFields.size)

    var filterField = ""
    var filterValue = ""
    var filterOp = ""

    field match {
      case "category" =>
        filterField = fields(field)
        filterValue = s""" "${getElementBySeed(filterFieldValue(field)("values"), valueSeed).toString}" """
        filterOp = "=="

      case _ => throw new Exception(s"Can not find field with name '$field' in filter operator generation.")
    }

    (
      buildFilterCode(inVarName, outVarName, filterField, filterValue, filterOp),
      getElementBySeed(filterFieldValue(field)("selectivity"), valueSeed).toString.toDouble
    )

  }


  def main(args: Array[String]): Unit = {
    for (i <- 0 to 3)
      for (j <- 0 to 3)
        println(filterCode("a", "b", i, j))

  }
}
