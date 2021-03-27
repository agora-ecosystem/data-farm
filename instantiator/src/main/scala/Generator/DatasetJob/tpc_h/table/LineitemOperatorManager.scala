package Generator.DatasetJob.tpc_h.table

import java.sql.Date

import Generator.DatasetJob.AbstractTableOperatorManager
import Generator.DatasetJob.utils.getElementBySeed

/**
  * Created by researchuser7 on 2020-03-04.
  */


case class LineitemOperatorManager() extends AbstractTableOperatorManager {

  val tableName = "lineitem"

  //override val tableCardinality: Int = 6001215

  val typeSchema = "(Int, Int, Int, Int, Float, Float, Float, Float, String, String, java.sql.Date, java.sql.Date, java.sql.Date, String, String)"

  val fields: Map[String, String] = Map(
    "ORDERKEY" -> "1",
    "PARTKEY" -> "2",
    "SUPPKEY" -> "3",
    "LINENUMBER" -> "4",
    "QUANTITY" -> "5",
    "EXTENDEDPRICE" -> "6",
    "DISCOUNT" -> "7",
    "TAX" -> "8",
    "RETURNFLAG" -> "9",
    "LINESTATUS" -> "10",
    "SHIPDATE" -> "11",
    "COMMITDATE" -> "12",
    "RECEIPTDATE" -> "13",
    "SHIPINSTRUCT" -> "14",
    "SHIPMODE" -> "15",
    "COMMENT" -> "16"
  )

  val joinFieldTable: Map[String, Map[String, String]] = Map(
    "ORDERKEY" -> Map(
      "orders" -> "ORDERKEY"
    ),
    "PARTKEY" -> Map(
      "part" -> "PARTKEY",
      "partsupp" -> "PARTKEY"
    ),
    "SUPPKEY" -> Map(
      "supplier" -> "SUPPKEY",
      "partsupp" -> "SUPPKEY"
    )
  )

  val filterFieldValue: Map[String, Map[String, Seq[String]]] = Map(
    "QUANTITY" -> Map(
      "selectivity" -> Seq("0.25", "0.5", "0.75"),
      "values" -> Seq("13.0", "26.0", "38.0")
    ),
    "SHIPDATE" -> Map(
      "selectivity" -> Seq("0.25", "0.5", "0.75"),
      "values" -> Seq("1993-10-26", "1995-06-19", "1997-02-09")
    ),
    "SHIPMODE" -> Map(
      "selectivity" -> Seq("0.1414", "0.1417", "0.1411", "0.1432", "0.1443", "0.1421", "0.1462"),
      "values" -> Seq("AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK")
    )
  )

  val groupFields: Map[String, String] = Map("ORDERKEY"-> "1500000", "PARTKEY"-> "200000", "SUPPKEY"-> "10000", "SHIPMODE"-> "7")

  override def dataSourceCode(outVarName: String): String = buildDataSourceCode(typeSchema, tableName + ".tbl", outVarName)


  def filterCode(inVarName: String, outVarName: String, fieldSeed: Int = 0, valueSeed: Int = 0): (String, Double) = {
    val field = filterFields(fieldSeed % filterFields.size)

    var filterField = ""
    var filterValue = ""
    var filterOp = ""

    field match {
      case "SHIPDATE" =>
        filterField = fields(field) + ".getTime"
        filterValue = s"""dateFormatter.parseDateTime("${getElementBySeed(filterFieldValue(field)("values"), valueSeed)}").getMillis"""
        filterOp = "<="

      case "QUANTITY" =>
        filterField = fields(field)
        filterValue = getElementBySeed(filterFieldValue(field)("values"), valueSeed).toString
        filterOp = "<="

      case "SHIPMODE" =>
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