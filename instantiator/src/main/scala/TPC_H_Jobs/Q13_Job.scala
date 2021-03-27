package TPC_H_Jobs

import java.text.SimpleDateFormat

import PlaygroundUtils.PlaygroundTools
import TPC_H.{CUSTOMER, NATION, ORDERS, PARTSUPP, SUPPLIER, TPC_H_utils}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row

/**
  * Created by researchuser7 on 2020-03-05.
  *
  * Query 13
  *
  * SELECT
  * C_COUNT,
  * COUNT(*) AS CUSTDIST
  * FROM (
  *
  * SELECT
  * C_CUSTKEY,
  * COUNT(O_ORDERKEY)
  * FROM CUSTOMER left outer join ORDERS on
  * C_CUSTKEY = O_CUSTKEY
  * AND O_COMMENT not like '%%special%%requests%%'
  * GROUP BY
  * C_CUSTKEY
  *
  * ) AS C_ORDERS (C_CUSTKEY, C_COUNT)
  * GROUP BY
  * C_COUNT
  * ORDER BY
  * CUSTDIST DESC,
  * C_COUNT DESC
  */
object Q13_Job {

  def run(params: Map[String, String]): Unit = {
    val env = if (params.getOrElse("local", "false") == "true") {
      val conf = new Configuration()
      conf.setString("taskmanager.heap.size", params.getOrElse("heap_size", "4G"))
      ExecutionEnvironment.createLocalEnvironment(conf)
    } else {
      ExecutionEnvironment.getExecutionEnvironment
    }

    // FROM CUSTOMER
    val customerData = env.readCsvFile[CUSTOMER](params("dataPath")+TPC_H_utils.TPC_CUSTOMER_TABLE, fieldDelimiter = "|")
    //customerData.first(10).print()

    // FROM ORDERS
    val ordersData = env.readCsvFile[ORDERS](params("dataPath")+TPC_H_utils.TPC_ORDERS_TABLE, fieldDelimiter = "|")
    //ordersData.first(10).print()

    val ordersFiltered = ordersData.filter(x => !x.O_COMMENT.matches(".*special.*requests.*"))

    //ordersFiltered.first(20).print()

    // CUSTOMER left outer join ORDERS on C_CUSTKEY = O_CUSTKEY
    val c_o_Joined = customerData.leftOuterJoin(ordersFiltered).where("C_CUSTKEY").equalTo("O_CUSTKEY") {
      (lx, rx) => (lx.C_CUSTKEY, if ((rx != null) && !rx.O_ORDERKEY.isNaN) 1 else 0)
    }

    //GROUP BY C_CUSTKEY
    val c_o_Grouped = c_o_Joined.groupBy(0)

    // SELECT C_COUNT, COUNT(*) AS CUSTDIST
    val matTable = c_o_Grouped.reduce((x1, x2) => (x1._1, x1._2 + x2._2))

    // GROUP BY C_COUNT
    val matTableGrouped = matTable.map(x => (x._2, 1)).groupBy(0)

    // SELECT C_COUNT, COUNT(*) AS CUSTDIST
    val matTableAggregated = matTableGrouped.reduce((x1, x2) => (x1._1, x1._2 + x2._2))

    // ORDER BY CUSTDIST DESC, C_COUNT DESC
    val queryResult = matTableAggregated
      .setParallelism(1)
      .sortPartition(1, Order.DESCENDING)
      .sortPartition(0, Order.DESCENDING)


    //Execution plan
    queryResult.output(new DiscardingOutputFormat())

    //Output
    val execPlan = env.getExecutionPlan()
    val execTime = if (params.getOrElse("execute", "false") == "true"){
      val execResult = env.execute
      println(StringContext("Runtime execution: ", " ms").s(execResult.getNetRuntime))
      Some(execResult.getNetRuntime)
    } else {
      None
    }

    PlaygroundTools.saveExecutionPlan("TPC_H_Q13", execPlan, execTime, params, env)

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

    // FROM CUSTOMER
    val customerData = env.readCsvFile[CUSTOMER](params("dataPath")+TPC_H_utils.TPC_CUSTOMER_TABLE, fieldDelimiter = "|")
    //customerData.first(10).print()

    // FROM ORDERS
    val ordersData = env.readCsvFile[ORDERS](params("dataPath")+TPC_H_utils.TPC_ORDERS_TABLE, fieldDelimiter = "|")

    tableEnv.createTemporaryView("CUSTOMER", customerData)
    tableEnv.createTemporaryView("ORDERS", ordersData)

    //val linitemTable = tableEnv.from("LINITEM")

    val result = tableEnv.sqlQuery(
      """
        | SELECT
        | C_COUNT,
        | COUNT(*) AS CUSTDIST
        | FROM (
        |
        | SELECT
        | C_CUSTKEY,
        | COUNT(O_ORDERKEY)
        | FROM CUSTOMER left outer join ORDERS on
        | C_CUSTKEY = O_CUSTKEY
        | AND O_COMMENT not like '%%special%%requests%%'
        | GROUP BY
        | C_CUSTKEY
        |
        | ) AS C_ORDERS (C_CUSTKEY, C_COUNT)
        | GROUP BY
        | C_COUNT
        | ORDER BY
        | CUSTDIST DESC,
        | C_COUNT DESC
      """.stripMargin)

    val queryResult = tableEnv.toDataSet[Row](result)
    queryResult.output(new DiscardingOutputFormat())

    //Output
    val execPlan = env.getExecutionPlan()
    val execTime = if (params.getOrElse("execute", "false") == "true"){
      val execResult = env.execute
      println(StringContext("Runtime execution: ", " ms").s(execResult.getNetRuntime))
      Some(execResult.getNetRuntime)
    } else {
      None
    }

    PlaygroundTools.saveExecutionPlan("TPC_H_Q13-SQL", execPlan, execTime, params, env)
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
