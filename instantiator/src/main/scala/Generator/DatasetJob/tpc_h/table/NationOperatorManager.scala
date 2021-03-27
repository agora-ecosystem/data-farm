package Generator.DatasetJob.tpc_h.table

import Generator.DatasetJob.AbstractTableOperatorManager
import Generator.DatasetJob.utils.getElementBySeed

/**
  * Created by researchuser7 on 2020-03-05.
  */
case class NationOperatorManager() extends AbstractTableOperatorManager {

  override val tableName: String = "nation"

  //override val tableCardinality: Int = 25

  override val typeSchema: String = "(Int, String, Int, String)"

  override val fields: Map[String, String] = Map(
    "NATIONKEY" -> "1",
    "NAME" -> "2",
    "REGIONKEY" -> "3",
    "COMMENT" -> "4"
  )
  override val joinFieldTable: Map[String, Map[String, String]] = Map(
    "NATIONKEY" -> Map(
      "customer" -> "NATIONKEY",
      "supplier" -> "NATIONKEY"
    )
  )

  override val filterFieldValue: Map[String, Map[String, Seq[String]]] = Map(
    "REGIONKEY"-> Map(
      "selectivity"-> Seq("0.25", "0.5", "0.75"),
      "values"-> Seq("1.0", "2.0", "3.0")))

  override val groupFields: Map[String, String] = Map("REGIONKEY"-> "5")

  override def dataSourceCode(outVarName: String): String = buildDataSourceCode(typeSchema, tableName + ".tbl", outVarName)

  override def filterCode(inVarName: String, outVarName: String, fieldSeed: Int, valueSeed: Int): (String, Double) = {
    val field = filterFields(fieldSeed % filterFields.size)

    var filterField = ""
    var filterValue = ""
    var filterOp = ""

    field match {
      case "REGIONKEY" =>
        filterField = fields(field)
        filterValue = getElementBySeed(filterFieldValue(field)("values"), valueSeed).toString
        filterOp = "=="

      case _ => throw new Exception(s"Can not find field with name '$field' in filter operator generation.")
    }

    (
      buildFilterCode(inVarName, outVarName, filterField, filterValue, filterOp),
      getElementBySeed(filterFieldValue(field)("selectivity"), valueSeed).toString.toDouble
    )
  }

}
