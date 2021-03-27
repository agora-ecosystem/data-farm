package Generator.DatasetJob.tpc_h.table

import Generator.DatasetJob.AbstractTableOperatorManager
import Generator.DatasetJob.utils.getElementBySeed

/**
  * Created by researchuser7 on 2020-03-05.
  */

case class PartsuppOperatorManager() extends AbstractTableOperatorManager{

  val tableName = "partsupp"

  //override val tableCardinality: Int = 800000

  override val typeSchema: String = "(Int, Int, Int, Float, String)"

  override val fields: Map[String, String] = Map(
    "PARTKEY" -> "1",
    "SUPPKEY" -> "2",
    "AVAILQTY" -> "3",
    "SUPPLYCOST" -> "4",
    "COMMENT" -> "5"
  )

  override val joinFieldTable: Map[String, Map[String, String]] = Map(
    "PARTKEY" -> Map(
      "part" -> "PARTKEY"
    ),
    "SUPPKEY" -> Map(
      "supplier" -> "SUPPKEY"
    )
  )

  override val filterFieldValue: Map[String, Map[String, Seq[String]]] = Map(
    "AVAILQTY"-> Map(
      "selectivity"-> Seq("0.25", "0.5", "0.75"),
      "values"-> Seq("2506.0", "5003.0", "7499.0")),
    "SUPPLYCOST"-> Map(
      "selectivity"-> Seq("0.25", "0.5", "0.75"),
      "values"-> Seq("250.81", "500.41", "750.2425")
    )
  )

  override val groupFields: Map[String, String] = Map("PARTKEY"-> "200000", "SUPPKEY"-> "10000")

  override def dataSourceCode(outVarName: String): String = buildDataSourceCode(typeSchema, tableName+".tbl", outVarName)

  override def filterCode(inVarName: String, outVarName: String, fieldSeed: Int, valueSeed: Int): (String, Double) = {
    val field = filterFields(fieldSeed % filterFields.size)

    var filterField = ""
    var filterValue = ""
    var filterOp = ""

    field match {

      case "AVAILQTY" =>
        filterField = fields(field)
        filterValue = getElementBySeed(filterFieldValue(field)("values"), valueSeed).toString
        filterOp = "<="

      case "SUPPLYCOST" =>
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
