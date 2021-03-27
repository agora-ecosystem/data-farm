package Generator.DatasetJob.tpc_h.table

import Generator.DatasetJob.AbstractTableOperatorManager
import Generator.DatasetJob.utils.getElementBySeed

/**
  * Created by researchuser7 on 2020-03-05.
  */
case class CustomerOperatorManager() extends AbstractTableOperatorManager {

  override val tableName: String = "customer"

  //override val tableCardinality: Int = 150000

  override val typeSchema: String = "(Int, String, String, Int, String, Float, String, String)"

  override val fields: Map[String, String] = Map(
    "CUSTKEY" -> "1",
    "NAME" -> "2",
    "ADDRESS" -> "3",
    "NATIONKEY" -> "4",
    "PHONE" -> "5",
    "ACCTBAL" -> "6",
    "MKTSEGMENT" -> "7",
    "COMMENT" -> "8"
  )

  override val joinFieldTable: Map[String, Map[String, String]] = Map(
    "CUSTKEY" -> Map(
      "orders" -> "CUSTKEY"
    ),
    "NATIONKEY" -> Map(
      "nation" -> "NATIONKEY"
    )
  )

  override val filterFieldValue: Map[String, Map[String, Seq[String]]] = Map(
    "ACCTBAL" -> Map(
      "selectivity" -> Seq("0.25", "0.5", "0.75"),
      "values" -> Seq("1757.6200000000001", "4477.3", "7246.3150000000005")),
    "MKTSEGMENT" -> Map(
      "selectivity" -> Seq("0.19834666666666667", "0.20094666666666666", "0.19978666666666667", "0.20126", "0.19966"),
      "values" -> Seq("AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY")
    )
  )

  override val groupFields: Map[String, String] = Map("MKTSEGMENT"-> "5")

  override def dataSourceCode(outVarName: String): String = buildDataSourceCode(typeSchema, tableName + ".tbl", outVarName)

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

      case "MKTSEGMENT" =>
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

}
