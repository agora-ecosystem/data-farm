package TPC_H_Jobs

import java.text.SimpleDateFormat

import PlaygroundUtils.PlaygroundTools
import TPC_H.{LINEITEM, NATION, ORDERS, PART, SUPPLIER, TPC_H_utils}
import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row

import scala.collection.JavaConverters._


/**
  * Created by researchuser7 on 2020-03-05.
  *
  * SELECT TOP 100
  * S_NAME,
  * COUNT(*) AS NUMWAIT
  * FROM
  * SUPPLIER,
  * LINEITEM L1,
  * ORDERS,
  * NATION
  * WHERE
  * S_SUPPKEY = L1.L_SUPPKEY
  * AND O_ORDERKEY = L1.L_ORDERKEY
  * AND O_ORDERSTATUS = 'F'
  * AND L1.L_RECEIPTDATE> L1.L_COMMITDATE
  * AND EXISTS (
  *
  * SELECT *
  * FROM
  * LINEITEM L2
  * WHERE
  * L2.L_ORDERKEY = L1.L_ORDERKEY
  * AND L2.L_SUPPKEY <> L1.L_SUPPKEY
  *
  * )
  * AND NOT EXISTS (
  *
  * SELECT *
  * FROM
  * LINEITEM L3
  * WHERE
  * L3.L_ORDERKEY = L1.L_ORDERKEY
  * AND L3.L_SUPPKEY <> L1.L_SUPPKEY
  * AND L3.L_RECEIPTDATE > L3.L_COMMITDATE
  *
  * )
  * AND S_NATIONKEY = N_NATIONKEY
  * AND N_NAME = 'SAUDI ARABIA'
  * GROUP BY
  * S_NAME
  * ORDER BY
  * NUMWAIT DESC,
  * S_NAME
  *
  */
object Q21_Job {

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

    // WHERE L1.L_RECEIPTDATE> L1.L_COMMITDATE
    val linitemFiltered = linitemData.filter(x => x.L_RECEIPTDATE.getTime > x.L_COMMITDATE.getTime)

    // FROM SUPPLIER
    val supplierData = env.readCsvFile[SUPPLIER](params("dataPath")+TPC_H_utils.TPC_SUPPLIER_TABLE, fieldDelimiter = "|")

    // FROM NATION
    val nationData = env.readCsvFile[NATION](params("dataPath")+TPC_H_utils.TPC_NATION_TABLE, fieldDelimiter = "|")

    // WHERE N_NAME = 'SAUDI ARABIA'
    val nationFiltered = nationData.filter(x => x.N_NAME == "SAUDI ARABIA")

    // FROM orders
    val ordersData = env.readCsvFile[ORDERS](params("dataPath")+TPC_H_utils.TPC_ORDERS_TABLE, fieldDelimiter = "|")

    // O_ORDERSTATUS = 'F'
    val ordersFiltered = ordersData.filter(x => x.O_ORDERSTATUS == "F")

    // WHERE S_NATIONKEY = N_NATIONKEY
    val s_n_Joined = supplierData.joinWithTiny(nationFiltered).where("S_NATIONKEY").equalTo("N_NATIONKEY") {
      (lx, rx) => (lx.S_SUPPKEY, lx.S_NAME)
    }

    // WHERE S_SUPPKEY = L1.L_SUPPKEY
    val s_n_l_Joined = linitemFiltered.join(s_n_Joined).where("L_SUPPKEY").equalTo(0) {
      (lx, rx) => (lx.L_ORDERKEY, lx.L_PARTKEY, lx.L_SUPPKEY, rx._2)
    }

    // WHERE O_ORDERKEY = L1.L_ORDERKEY
    val s_n_l_o_Joined = ordersFiltered.join(s_n_l_Joined).where("O_ORDERKEY").equalTo(0) {
      (lx, rx) => rx // L_ORDERKEY, L_PARTKEY, L_SUPPKEY, S_NAME
    }

    //s_n_l_o_Joined.first(20).print()
    //println()

    /* Inner query 1 -> Exists
     *
     * SELECT *
     * FROM
     *   LINEITEM L2
     * WHERE
     *   L2.L_ORDERKEY = L1.L_ORDERKEY
     *   AND L2.L_SUPPKEY <> L1.L_SUPPKEY
     */

    //    //TODO rewrite with hashmap of L_ORDERKEY
    //    val linitemExists1 = s_n_l_o_Joined.joinWithHuge(linitemData).where(0).equalTo("L_ORDERKEY"){
    //      (lx, rx) => (
    //        lx._1, // L_ORDERKEY
    //        lx._2, // L_PARTKEY
    //        lx._3, // L_SUPPKEY
    //        lx._4, // S_NAME
    //        if (rx.L_SUPPKEY != lx._3) 1 else 0)
    //    }.groupBy(0, 1, 2)
    //      .sum(4)
    //      .filter(_._5 > 0)
    //      .map(x => (x._1, x._2, x._3, x._4))


    val linitemExists1 = s_n_l_o_Joined.joinWithHuge(linitemData).where(0).equalTo("L_ORDERKEY") {
      (lx, rx) =>
        (
          lx._1, // L_ORDERKEY
          if (rx.L_SUPPKEY != lx._3) 1 else 0)
    }.groupBy(0)
      .sum(1)
      .filter(_._2 > 0)
      .map(x => x._1)

    //linitemExists1.first(10).print()
    //println()

    // WHERE EXISTS (...)
    val s_n_l_o_Exists1 = s_n_l_o_Joined.filter(new RichFilterFunction[(Int, Int, Int, String)] {
      var broadcastSet: Set[Int] = _

      override def open(config: Configuration): Unit = {
        broadcastSet = getRuntimeContext.getBroadcastVariable[Int]("exists1LookupSet").asScala.toSet
      }

      override def filter(x: (Int, Int, Int, String)): Boolean = {
        if (broadcastSet.contains(x._1)) true else false
      }
    }).withBroadcastSet(linitemExists1, "exists1LookupSet")

    //linitemExists1.first(20).print()

    /* Inner query 1 -> Not Exists
     *
     * SELECT *
     * FROM
     *   LINEITEM L3
     * WHERE
     *   L3.L_ORDERKEY = L1.L_ORDERKEY
     *   AND L3.L_SUPPKEY <> L1.L_SUPPKEY
     *   AND L3.L_RECEIPTDATE > L3.L_COMMITDATE
     */

    //    val linitemExists2 = linitemExists1.joinWithHuge(linitemData).where(0).equalTo("L_ORDERKEY"){
    //      (lx, rx) => (
    //        lx._1, // L_ORDERKEY
    //        lx._2, // L_PARTKEY
    //        lx._3, // L_SUPPKEY
    //        lx._4, // S_NAME
    //        if ((rx.L_SUPPKEY != lx._3) && (rx.L_RECEIPTDATE.getTime > rx.L_COMMITDATE.getTime)) 1 else 0)
    //    }.groupBy(0, 1, 2)
    //      .sum(4)
    //      .filter(_._5 == 0)
    //      .map(x => (x._1, x._2, x._3, x._4))


    val linitemExists2 = s_n_l_o_Joined.joinWithHuge(linitemData).where(0).equalTo("L_ORDERKEY") {
      (lx, rx) =>
        (
          lx._1, // L_ORDERKEY
          if ((rx.L_SUPPKEY != lx._3) && (rx.L_RECEIPTDATE.getTime > rx.L_COMMITDATE.getTime)) 1 else 0)
    }.groupBy(0)
      .sum(1)
      .filter(_._2 == 0)
      .map(x => x._1)

    //linitemExists2.first(10).print()

    // WHERE NOT EXISTS (...)
    val s_n_l_o_Exists2 = s_n_l_o_Exists1.filter(new RichFilterFunction[(Int, Int, Int, String)] {
      var broadcastSet: Set[Int] = _

      override def open(config: Configuration): Unit = {
        broadcastSet = getRuntimeContext.getBroadcastVariable[Int]("exists2LookupSet").asScala.toSet
      }

      override def filter(x: (Int, Int, Int, String)): Boolean = {
        if (broadcastSet.contains(x._1)) true else false
      }
    }).withBroadcastSet(linitemExists2, "exists2LookupSet")

    // GROUP BY S_NAME
    val linitemExists2Grouped = s_n_l_o_Exists2.map(x => (x._4, 1)).groupBy(0)

    // SELECT TOP 100 S_NAME, COUNT(*) AS NUMWAIT
    val queryResult = linitemExists2Grouped
      .reduce((x1, x2) => (x1._1, x1._2 + x2._2))
      .setParallelism(1)
      .sortPartition(1, Order.DESCENDING)
      .sortPartition(0, Order.ASCENDING)
      .first(100)

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

    PlaygroundTools.saveExecutionPlan("TPC_H_Q21", execPlan, execTime, params, env)

  }

  def run_sql(params: Map[String, String]): Unit ={
    import org.apache.flink.configuration.ConfigConstants

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

    // FROM SUPPLIER
    val supplierData = env.readCsvFile[SUPPLIER](params("dataPath")+TPC_H_utils.TPC_SUPPLIER_TABLE, fieldDelimiter = "|")

    // FROM NATION
    val nationData = env.readCsvFile[NATION](params("dataPath")+TPC_H_utils.TPC_NATION_TABLE, fieldDelimiter = "|")

    // FROM orders
    val ordersData = env.readCsvFile[ORDERS](params("dataPath")+TPC_H_utils.TPC_ORDERS_TABLE, fieldDelimiter = "|")

    tableEnv.createTemporaryView("LINEITEM", linitemData)
    tableEnv.createTemporaryView("SUPPLIER", supplierData)
    tableEnv.createTemporaryView("NATION", nationData)
    tableEnv.createTemporaryView("ORDERS", ordersData)

    val result = tableEnv.sqlQuery(
      """
        | SELECT
        | S_NAME,
        | COUNT(*) AS NUMWAIT
        | FROM
        | SUPPLIER,
        | LINEITEM L1,
        | ORDERS,
        | NATION
        | WHERE
        | S_SUPPKEY = L1.L_SUPPKEY
        | AND O_ORDERKEY = L1.L_ORDERKEY
        | AND O_ORDERSTATUS = 'F'
        | AND L1.L_RECEIPTDATE> L1.L_COMMITDATE
        | AND EXISTS (
        |
        | SELECT *
        | FROM
        | LINEITEM L2
        | WHERE
        | L2.L_ORDERKEY = L1.L_ORDERKEY
        | AND L2.L_SUPPKEY <> L1.L_SUPPKEY
        |
        | )
        | AND NOT EXISTS (
        |
        | SELECT *
        | FROM
        | LINEITEM L3
        | WHERE
        | L3.L_ORDERKEY = L1.L_ORDERKEY
        | AND L3.L_SUPPKEY <> L1.L_SUPPKEY
        | AND L3.L_RECEIPTDATE > L3.L_COMMITDATE
        |
        | )
        | AND S_NATIONKEY = N_NATIONKEY
        | AND N_NAME = 'SAUDI ARABIA'
        | GROUP BY
        | S_NAME
        | ORDER BY
        | NUMWAIT DESC,
        | S_NAME
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

    PlaygroundTools.saveExecutionPlan("TPC_H_Q21-SQL", execPlan, execTime, params, env)
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
