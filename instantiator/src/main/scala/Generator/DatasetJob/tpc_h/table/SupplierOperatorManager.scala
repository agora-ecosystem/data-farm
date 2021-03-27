package Generator.DatasetJob.tpc_h.table

import Generator.DatasetJob.AbstractTableOperatorManager
import Generator.DatasetJob.utils.getElementBySeed

/**
  * Created by researchuser7 on 2020-03-05.
  */

case class SupplierOperatorManager() extends AbstractTableOperatorManager{

  override val tableName: String = "supplier"

  //override val tableCardinality: Int = 10000

  override val typeSchema: String = "(Int, String, String, Int, String, Float, String)"

  override val fields: Map[String, String] = Map(
    "SUPPKEY" -> "1",
    "NAME" -> "2",
    "ADDRESS" -> "3",
    "NATIONKEY" -> "4",
    "PHONE" -> "5",
    "ACCTBAL" -> "6",
    "COMMENT" -> "7"
  )

  override val joinFieldTable: Map[String, Map[String, String]] = Map(
    "SUPPKEY"-> Map(
      "partsupp" -> "SUPPKEY",
      "lineitem" -> "SUPPKEY"
    ),
    "NATIONKEY"-> Map(
      "nation" -> "NATIONKEY"
    )
  )

  override val filterFieldValue: Map[String, Map[String, Seq[String]]] = Map(
    "ACCTBAL"-> Map(
      "selectivity"-> Seq("0.25", "0.5", "0.75"),
      "values"-> Seq("1770.7175", "4541.0650000000005", "7270.635")),
    "NATIONKEY"-> Map(
      "selectivity"-> Seq("0.25", "0.5", "0.75"),
      "values"-> Seq("6.0", "12.0", "18.0")
    )
  )

  override val groupFields: Map[String, String] = Map("NATIONKEY"-> "25")

  override def dataSourceCode(outVarName: String): String = buildDataSourceCode(typeSchema, tableName+".tbl", outVarName)

  override def filterCode(inVarName: String, outVarName: String, fieldSeed: Int, valueSeed: Int): (String, Double) = {
    val field = filterFields(fieldSeed % filterFields.size)

    var filterField = ""
    var filterValue = ""
    var filterOp = ""

    field match {
      case "ACCTBAL" =>
        filterField = fields(field)
        filterValue = getElementBySeed(filterFieldValue(field)("values"), valueSeed).toString
        filterOp = "<="

      case "NATIONKEY" =>
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