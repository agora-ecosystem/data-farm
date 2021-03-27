package Generator.DatasetJob.tpc_h.table

import java.sql.Date

import Generator.DatasetHelper
import Generator.DatasetJob.AbstractTableOperatorManager
import Generator.DatasetJob.utils.getElementBySeed

/**
  * Created by researchuser7 on 2020-03-05.
  */
case class OrdersOperatorManager() extends AbstractTableOperatorManager {

  override val tableName: String = "orders"

  //override val tableCardinality: Int = 1500000

  override val typeSchema: String = "(Int, Int, String, Float, java.sql.Date, String, String, Int, String)"
  override val fields: Map[String, String] = Map(
    "ORDERKEY" -> "1",
    "CUSTKEY" -> "2",
    "ORDERSTATUS" -> "3",
    "TOTALPRICE" -> "4",
    "ORDERDATE" -> "5",
    "ORDERPRIORITY" -> "6",
    "CLERK" -> "7",
    "SHIPPRIORITY" -> "8",
    "COMMENT" -> "9"
  )
  override val filterFieldValue: Map[String, Map[String, Seq[String]]] = Map(
    "ORDERSTATUS" -> Map(
      "selectivity" -> Seq("0.48627533333333334", "0.4880293333333333", "0.025695333333333334"),
      "values" -> Seq("F", "O", "P")),
    "TOTALPRICE" -> Map(
      "selectivity" -> Seq("0.25", "0.5", "0.75"),
      "values" -> Seq("77894.7475", "144409.03999999998", "215500.225")),
    "ORDERDATE" -> Map(
      "selectivity" -> Seq("0.25", "0.5", "0.75"),
      "values" -> Seq("1993-08-27", "1995-04-20", "1996-12-10")
    )
  )


  val joinFieldTable: Map[String, Map[String, String]] = Map(
    "ORDERKEY" -> Map(
      "lineitem" -> "ORDERKEY"
    ),
    "CUSTKEY" -> Map(
      "customer" -> "CUSTKEY"
    )
  )

  override val groupFields: Map[String, String] = Map("ORDERKEY"-> "1500000", "CUSTKEY"-> "99996", "ORDERSTATUS"-> "3")

  override def dataSourceCode(outVarName: String): String = buildDataSourceCode(typeSchema, tableName + ".tbl", outVarName)

  override def filterCode(inVarName: String, outVarName: String, fieldSeed: Int, valueSeed: Int): (String, Double) = {

    //(s"""val $outVarName = $inVarName.filter(x=>x._5.getTime <= (dateFormatter.parseDateTime("1998-12-01").getMillis - 90L * 24L * 60L * 60L * 1000L))""", -1.0)

    val field = filterFields(fieldSeed % filterFields.size)

    var filterField = ""
    var filterValue = ""
    var filterOp = ""

    field match {

      case "ORDERDATE" =>
        filterField = fields(field) + ".getTime"
        filterValue = s"""dateFormatter.parseDateTime("${getElementBySeed(filterFieldValue(field)("values"), valueSeed)}").getMillis"""
        filterOp = "<="

      case "TOTALPRICE" =>
        filterField = fields(field)
        filterValue = getElementBySeed(filterFieldValue(field)("values"), valueSeed).toString
        filterOp = "<="

      case "ORDERSTATUS" =>
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
