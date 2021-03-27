package Generator.DatasetJob.tpc_h.table

import Generator.DatasetJob.AbstractTableOperatorManager
import Generator.DatasetJob.utils.getElementBySeed

/**
  * Created by researchuser7 on 2020-03-05.
  */

case class PartOperatorManager() extends AbstractTableOperatorManager {

  override val tableName: String = "part"

  //override val tableCardinality: Int = 200000

  override val typeSchema: String = "(Int, String, String, String, String, Int, String, Float, String)"

  override val fields: Map[String, String] = Map(
    "PARTKEY" -> "1",
    "NAME" -> "2",
    "MFGR" -> "3",
    "BRAND" -> "4",
    "TYPE" -> "5",
    "SIZE" -> "6",
    "CONTAINER" -> "7",
    "RETAILPRICE" -> "8",
    "COMMENT" -> "9"
  )

  override val joinFieldTable: Map[String, Map[String, String]] = Map(
    "PARTKEY" -> Map(
      "partsupp" -> "PARTKEY",
      "lineitem" -> "PARTKEY"
    )
  )

  override val filterFieldValue: Map[String, Map[String, Seq[String]]] = Map(
    "RETAILPRICE" -> Map(
      "selectivity" -> Seq("0.25", "0.5", "0.75"),
      "values" -> Seq("1249.2475", "1499.495", "1749.7425")
    )
  )

  override val groupFields: Map[String, String] = Map("BRAND"-> "25", "TYPE"-> "150")



  override def dataSourceCode(outVarName: String): String = buildDataSourceCode(typeSchema, tableName + ".tbl", outVarName)

  override def filterCode(inVarName: String, outVarName: String, fieldSeed: Int, valueSeed: Int): (String, Double) = {
    val field = filterFields(fieldSeed % filterFields.size)

    var filterField = ""
    var filterValue = ""
    var filterOp = ""

    field match {

      case "RETAILPRICE" =>
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
