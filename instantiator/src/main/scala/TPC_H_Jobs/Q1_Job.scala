package TPC_H_Jobs

import java.text.SimpleDateFormat

import PlaygroundUtils.PlaygroundTools
import TPC_H.{LINEITEM, TPC_H_utils}
import org.apache.flink.api.scala._
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.descriptors.FileSystem
import org.apache.flink.types.Row
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by researchuser7 on 2020-03-04.
  */
object Q1_Job {


  /**
    * Query 1
    *
    * SELECT
    * l_returnflag,
    * l_linestatus,
    * sum(l_quantity) as sum_qty,
    * sum(l_extendedprice) as sum_base_price,
    * sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    * sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    * avg(l_quantity) as avg_qty,
    * avg(l_extendedprice) as avg_price,
    * avg(l_discount) as avg_disc,
    * count(*) as count_order
    * FROM
    * lineitem
    * WHERE
    * l_shipdate <= date '1998-12-01' - interval '90' day
    * GROUP BY
    * l_returnflag,
    * l_linestatus
    * ORDER BY
    * l_returnflag,
    * l_linestatus;
    */

  //TODO replace with POJO
  //private val TPC_LINITEM_TABLE: String = "file:///Users/researchuser7/IdeaProjects/flink-playground-jobs/misc/tpc-h-tool/2.18.0_rc2/ref_data/1/lineitem.tbl.1"
  //private val TPC_LINITEM_TABLE: String = "file:///Users/researchuser7/Desktop/tpc-test/db_out/lineitem.tbl"

  //private val LOG: Logger = LoggerFactory.getLogger(Q1_Job.getClass)

  //private val outPath = s"/Users/researchuser7/IdeaProjects/flink-playground-jobs/out/tpc_h_q1/${new SimpleDateFormat("yyyyMMdd_HHmmss").format(new java.util.Date())}"

  private val formatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")

  def run(params: Map[String, String]): Unit ={
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")


    val env = if (params.getOrElse("local", "false") == "true") {
      val conf = new Configuration()
      conf.setString("taskmanager.heap.size", params.getOrElse("heap_size", "4G"))
      ExecutionEnvironment.createLocalEnvironment(conf)
    } else {
      ExecutionEnvironment.getExecutionEnvironment
    }

    // FROM lineitem
    //val data: DataSet[String] = env.readTextFile("TPC_LINITEM_TABLE")
    val data = env.readCsvFile[(Int, Int, Int, Int, Float, Float, Float, Float, String, String, java.sql.Date, java.sql.Date, java.sql.Date, String, String)](params("dataPath")+TPC_H_utils.TPC_LINITEM_TABLE, fieldDelimiter = "|")
    val parsedData = data //data.map(x => x.copy(_11 = format.parse(x._11), _12 = format.parse(x._12), _13 = format.parse(x._13)))
    //println(parsedData.count())
    //println()

    // WHERE l_shipdate <= date '1998-12-01' - interval '90' day
    val filteredData = parsedData.filter(x => x._11.getTime <= (TPC_H_utils.formatter.parseDateTime("1998-12-01").getMillis - 90L * 24L * 60L * 60L * 1000L)) // 90*24*60*60*1000 -> 90 days
    //filteredData.print()
    //println("filter:" + filteredData.count())

    // GROUP BY #Version 2
    val groupedData2 = filteredData
      .map(x => (
        x._9, // l_returnflag
        x._10, // l_linestatus
        x._5, // l_quantity
        x._6, // l_extendedprice
        x._6 * (1 - x._7), // l_extendedprice * (1 - l_discount)
        x._6 * (1 - x._7) * (1 + x._8), // l_extendedprice * (1 - l_discount) * (1 + l_tax)
        x._5, // l_quantity
        x._6, // l_extendedprice
        x._7, // l_discount
        1 // count()
      ))
      .groupBy(0, 1) // l_returnflag, l_linestatus

    //print(groupedData2.first(1).print())
    //println("groupedData2:" + filteredData.map(x => (x._9, x._10, 1)).groupBy(0,1).sum(2).count())

    val aggregatedResult = groupedData2
      .reduce((x1, x2) =>
        (
          x1._1, // l_returnflag
          x1._2, // l_linestatus
          x1._3 + x2._3, // sum(l_quantity)
          x1._4 + x2._4, // sum(l_extendedprice)
          x1._5 + x2._5, // sum(l_extendedprice * (1 - l_discount))
          x1._6 + x2._6, //sum(l_extendedprice * (1 - l_discount) * (1 + l_tax))
          x1._7 + x2._7, // sum(l_quantity)
          x1._8 + x2._8, // sum(l_extendedprice)
          x1._9 + x2._9, // sum(l_discount)
          x1._10 + x2._10 // count()
        ))
      .map(x => x.copy(_7 = x._7 / x._10, _8 = x._8 / x._10, _9 = x._9 / x._10))

    //aggregatedResult.print()

    // ORDER BY l_returnflag, l_linestatus
    val queryResult = aggregatedResult
      .setParallelism(1)
      .sortPartition(0,Order.ASCENDING)
      .sortPartition(1, Order.ASCENDING)

    println("queryResult:" + queryResult.count())

    queryResult.output(new DiscardingOutputFormat())

    val execPlan = env.getExecutionPlan()
    val execTime = if (params.getOrElse("execute", "false") == "true"){
      val execResult = env.execute
      println(StringContext("Runtime execution: ", " ms").s(execResult.getNetRuntime))
      Some(execResult.getNetRuntime)
    } else {
      None
    }

    PlaygroundTools.saveExecutionPlan("TPC_H_Q1", execPlan, execTime, params, env)
  }

  def run_sql(params:Map[String,String]): Unit ={
    val env = if (params.getOrElse("local", "false") == "true") {
      val conf = new Configuration()
      conf.setString("taskmanager.heap.size", params.getOrElse("heap_size", "4G"))
      ExecutionEnvironment.createLocalEnvironment(conf)
    } else {
      ExecutionEnvironment.getExecutionEnvironment
    }

    val tableEnv = BatchTableEnvironment.create(env)

    // FROM lineitem
    val linitemData = env.readCsvFile[LINEITEM](params("dataPath")+TPC_H_utils.TPC_LINITEM_TABLE, fieldDelimiter = "|")

    tableEnv.createTemporaryView("LINITEM", linitemData)

    val linitemTable = tableEnv.from("LINITEM")

    val result = tableEnv.sqlQuery(
      """
        | SELECT
        | L_RETURNFLAG,
        | L_LINESTATUS,
        | sum(L_QUANTITY) as sum_qty,
        | sum(L_EXTENDEDPRICE) as sum_base_price,
        | sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as sum_disc_price,
        | sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT) * (1 + L_TAX)) as sum_charge,
        | avg(L_QUANTITY) as avg_qty,
        | avg(L_EXTENDEDPRICE) as avg_price,
        | avg(L_DISCOUNT) as avg_disc,
        | count(*) as count_order
        | FROM
        | LINITEM
        | WHERE
        | L_SHIPDATE <= date '1998-12-01' - interval '90' day
        | GROUP BY
        | L_RETURNFLAG,
        | L_LINESTATUS
        | ORDER BY
        | L_RETURNFLAG,
        | L_LINESTATUS
      """.stripMargin)

    val queryResult = tableEnv.toDataSet[Row](result)
    queryResult.output(new DiscardingOutputFormat())

    val execPlan = env.getExecutionPlan()
    val execTime = if (params.getOrElse("execute", "false") == "true"){
      val execResult = env.execute
      println(StringContext("Runtime execution: ", " ms").s(execResult.getNetRuntime))
      Some(execResult.getNetRuntime)
    } else {
      None
    }

    PlaygroundTools.saveExecutionPlan("TPC_H_Q1-SQL", execPlan, execTime, params, env)
  }

  def main(args: Array[String]): Unit = {
    val params = if (args.length==4){
      Map(
        "dataPath" -> args(0),
        "execPlanOutPath" -> args(1),
        "execute" -> (args(2) == "exec").toString,
        "local" -> (args(3) == "local").toString
      )
    }
    else {
      throw new Exception("Expected arguments: <data_path> <exec_plan_out_path> <exec|noexec> <local>")
    }
    run(params)
    //run_sql(params)
  }

}
