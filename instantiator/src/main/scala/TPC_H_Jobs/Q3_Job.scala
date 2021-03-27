package TPC_H_Jobs

import java.sql.Date
import java.text.SimpleDateFormat

import PlaygroundUtils.PlaygroundTools
import TPC_H.{CUSTOMER, LINEITEM, ORDERS, TPC_H_utils}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}


/**
  * Created by researchuser7 on 2020-03-05.
  *
  * Query 3
  *
  * SELECT
  * l_orderkey,
  * sum(l_extendedprice * (1 - l_discount)) as revenue,
  * o_orderdate,
  * o_shippriority
  * FROM
  * customer,
  * orders,
  * lineitem
  * WHERE
  * c_mktsegment = 'BUILDING'
  * AND c_custkey = o_custkey
  * AND l_orderkey = o_orderkey
  * AND o_orderdate < date '1995-03-15'
  * AND l_shipdate > date '1995-03-15'
  * GROUP BY
  * l_orderkey,
  * o_orderdate,
  * o_shippriority
  * ORDER BY
  * revenue desc,
  * o_orderdate
  * LIMIT 20;
  */
object Q3_Job {

  def run(params: Map[String, String]): Unit = {
    val env = if (params.getOrElse("local", "false") == "true") {
      val conf = new Configuration()
      conf.setString("taskmanager.heap.size", params.getOrElse("heap_size", "4G"))
      ExecutionEnvironment.createLocalEnvironment(conf)
    } else {
      ExecutionEnvironment.getExecutionEnvironment
    }

    // FROM lineitem
    val linitemData = env.readCsvFile[LINEITEM](params("dataPath")+TPC_H_utils.TPC_LINITEM_TABLE, fieldDelimiter = "|")

    // WHERE l_shipdate > date '1995-03-15'
    val linitemFiltered = linitemData.filter(x => x.L_SHIPDATE.getTime > TPC_H_utils.formatter.parseDateTime("1995-03-15").getMillis)

    // FROM customer
    val customerData = env.readCsvFile[CUSTOMER](params("dataPath")+TPC_H_utils.TPC_CUSTOMER_TABLE, fieldDelimiter = "|")

    // WHERE c_mktsegment = 'BUILDING'
    val customerFiltered = customerData.filter(x => x.C_MKTSEGMENT == "BUILDING")
    //customerFiltered.print()

    // FROM orders
    val ordersData = env.readCsvFile[ORDERS](params("dataPath")+TPC_H_utils.TPC_ORDERS_TABLE, fieldDelimiter = "|")

    // WHERE o_orderdate < date '1995-03-15'
    val ordersFiltered = ordersData.filter(x => x.O_ORDERDATE.getTime < TPC_H_utils.formatter.parseDateTime("1995-03-15").getMillis)

    // WHERE c_custkey = o_custkey
    val c_o_JoinData = customerFiltered.join(ordersFiltered).where("C_CUSTKEY").equalTo("O_CUSTKEY") {
      (lx, rx, out: Collector[(Int, Date, Int)]) => out.collect((rx.O_ORDERKEY, rx.O_ORDERDATE, rx.O_SHIPPRIORITY))
    }

    // WHERE l_orderkey = o_orderkey
    val l_c_o_JoinData = c_o_JoinData.join(linitemFiltered).where(0).equalTo("L_ORDERKEY") {
      (lx, rx, out: Collector[(Int, Float, Date, Int)]) =>
        out.collect(
          (
            rx.L_ORDERKEY,
            rx.L_EXTENDEDPRICE * (1 - rx.L_DISCOUNT),
            lx._2, // O_ORDERDATE
            lx._3 // O_SHIPPRIORITY
          )
        )
    }

    //GROUP BY l_orderkey, o_orderdate, o_shippriority
    val l_c_o_GroupData = l_c_o_JoinData.groupBy(0, 2, 3)

    // SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority
    val l_c_o_AggregateData = l_c_o_GroupData.reduce((x1, x2) => (x1._1, x1._2 + x2._2, x1._3, x1._4))

    // ORDER BY  revenue desc, o_orderdate
    val queryResult = l_c_o_AggregateData
      .setParallelism(1)
      .sortPartition(1, Order.DESCENDING)
      .sortPartition(2, Order.ASCENDING)

    //Execution plan
    queryResult.output(new DiscardingOutputFormat())

    val execPlan = env.getExecutionPlan()
    val execTime = if (params.getOrElse("execute", "false") == "true"){
      val execResult = env.execute
      println(StringContext("Runtime execution: ", " ms").s(execResult.getNetRuntime))
      Some(execResult.getNetRuntime)
    } else {
      None
    }

    PlaygroundTools.saveExecutionPlan("TPC_H_Q3", execPlan, execTime, params, env)
  }

  def run_sql(params: Map[String, String]): Unit ={
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

    // FROM customer
    val customerData = env.readCsvFile[CUSTOMER](params("dataPath")+TPC_H_utils.TPC_CUSTOMER_TABLE, fieldDelimiter = "|")

    // FROM orders
    val ordersData = env.readCsvFile[ORDERS](params("dataPath")+TPC_H_utils.TPC_ORDERS_TABLE, fieldDelimiter = "|")

    tableEnv.createTemporaryView("LINEITEM", linitemData)
    tableEnv.createTemporaryView("CUSTOMER", customerData)
    tableEnv.createTemporaryView("ORDERS", ordersData)

    //val linitemTable = tableEnv.from("LINITEM")

    val result = tableEnv.sqlQuery(
      """
        | SELECT
        | L_ORDERKEY,
        | SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE,
        | O_ORDERDATE,
        | O_SHIPPRIORITY
        | FROM
        | CUSTOMER,
        | ORDERS,
        | LINEITEM
        | WHERE
        | C_MKTSEGMENT = 'BUILDING'
        | AND C_CUSTKEY = O_CUSTKEY
        | AND L_ORDERKEY = O_ORDERKEY
        | AND O_ORDERDATE < DATE '1995-03-15'
        | AND L_SHIPDATE > DATE '1995-03-15'
        | GROUP BY
        | L_ORDERKEY,
        | O_ORDERDATE,
        | O_SHIPPRIORITY
        | ORDER BY
        | REVENUE DESC,
        | O_ORDERDATE
        | LIMIT 20
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

    PlaygroundTools.saveExecutionPlan("TPC_H_Q3-SQL", execPlan, execTime, params, env)
  }

  def main(args: Array[String]): Unit = {
    val params = if (args.length==3){
      Map(
        "dataPath" -> args(0),
        "execPlanOutPath" -> args(1),
        "execute" -> (args(2) == "exec").toString
      )
    }
    else {
      throw new Exception("Expected arguments: <data_path> <exec_plan_out_path> <exec|noexec>")
    }
    run(params)
    //run_sql(params)
  }

}
