package TPC_H_Jobs

import java.text.SimpleDateFormat

import PlaygroundUtils.PlaygroundTools
import org.apache.flink.api.scala._
import TPC_H.{NATION, PARTSUPP, SUPPLIER, TPC_H_utils}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.io.DiscardingOutputFormat
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
  * Created by researchuser7 on 2020-03-05.
  *
  * Query 11
  *
  * SELECT
  * PS_PARTKEY,
  * SUM(PS_SUPPLYCOST*PS_AVAILQTY) AS VALUE
  * FROM
  * PARTSUPP,
  * SUPPLIER,
  * NATION
  * WHERE
  * PS_SUPPKEY = S_SUPPKEY
  * AND S_NATIONKEY = N_NATIONKEY
  * AND N_NAME = 'GERMANY'
  * GROUP BY
  * PS_PARTKEY
  * HAVING
  * SUM(PS_SUPPLYCOST*PS_AVAILQTY) > (
  * SELECT
  * SUM(PS_SUPPLYCOST*PS_AVAILQTY) * 0.0001000000
  * FROM
  * PARTSUPP,
  * SUPPLIER,
  * NATION
  * WHERE
  * PS_SUPPKEY = S_SUPPKEY
  * AND S_NATIONKEY = N_NATIONKEY AND
  * N_NAME = 'GERMANY'
  * )
  * ORDER BY VALUE DESC
  *
  */
object Q11_Job {

  def run(params: Map[String, String]): Unit = {

    val env = if (params.getOrElse("local", "false") == "true") {
      val conf = new Configuration()
      conf.setString("taskmanager.heap.size", params.getOrElse("heap_size", "4G"))
      ExecutionEnvironment.createLocalEnvironment(conf)
    } else {
      ExecutionEnvironment.getExecutionEnvironment
    }

    // FROM PARTSUP
    val partsuppData = env.readCsvFile[PARTSUPP](params("dataPath")+TPC_H_utils.TPC_PARTSUPP_TABLE, fieldDelimiter = "|")

    // FROM SUPPLIER
    val supplierData = env.readCsvFile[SUPPLIER](params("dataPath")+TPC_H_utils.TPC_SUPPLIER_TABLE, fieldDelimiter = "|")

    // FROM NATION
    val nationData = env.readCsvFile[NATION](params("dataPath")+TPC_H_utils.TPC_NATION_TABLE, fieldDelimiter = "|")

    //WHERE N_NAME = 'GERMANY'
    val nationFiltered = nationData.filter(x => x.N_NAME == "GERMANY")

    // WHERE S_NATIONKEY = N_NATIONKEY
    // It can be implemented as a simple filter since there is only 1 nation in nationFiltered
    val s_n_JoinData = supplierData.joinWithTiny(nationFiltered).where("S_NATIONKEY").equalTo("N_NATIONKEY") {
      (lx, rx, out: Collector[(Int, Int)]) =>
        out.collect(
          (
            lx.S_SUPPKEY,
            lx.S_NATIONKEY
          )
        )
    }

    // WHERE PS_SUPPKEY = S_SUPPKEY
    val ps_s_n_JoinData = partsuppData.join(s_n_JoinData).where("PS_SUPPKEY").equalTo(0) {
      (lx, rx, out: Collector[(Int, Float)]) =>
        out.collect(
          (
            lx.PS_PARTKEY,
            lx.PS_SUPPLYCOST * lx.PS_AVAILQTY // PS_SUPPLYCOST*PS_AVAILQTY
          )
        )
    }

    //ps_s_n_JoinData.first(20).print()

    /* Having query
    * SELECT
    *   SUM(PS_SUPPLYCOST*PS_AVAILQTY) * 0.0001000000
    * FROM
    *   PARTSUPP,
    *   SUPPLIER,
    *   NATION
    * WHERE
    *   PS_SUPPKEY = S_SUPPKEY
    *   AND S_NATIONKEY = N_NATIONKEY AND
    *   N_NAME = 'GERMANY'
    */
    val havingQueryResult = ps_s_n_JoinData
      .map(_._2)
      .reduce(_ + _).collect().head * 0.00010f

    //GROUP BY PS_PARTKEY
    val ps_s_n_GoupedData = ps_s_n_JoinData.groupBy(0)
    val ps_s_n_AggregatedData = ps_s_n_GoupedData.reduce((x1, x2) => (x1._1, x1._2 + x2._2))

    //HAVING SUM(PS_SUPPLYCOST*PS_AVAILQTY) > (...)
    val ps_s_n_FilteredAggregatedData = ps_s_n_AggregatedData.filter(x => x._2 > havingQueryResult)

    // ORDER BY VALUE DESC
    val queryResult = ps_s_n_FilteredAggregatedData.setParallelism(1)
      .sortPartition(1, Order.DESCENDING)

    //Execution plan
    queryResult.output(new DiscardingOutputFormat())
    //print(env.getExecutionPlan())

    val execPlan = env.getExecutionPlan()
    val execTime = if (params.getOrElse("execute", "false") == "true"){
      val execResult = env.execute
      println(StringContext("Runtime execution: ", " ms").s(execResult.getNetRuntime))
      Some(execResult.getNetRuntime)
    } else {
      None
    }

    PlaygroundTools.saveExecutionPlan("TPC_H_Q11", execPlan, execTime, params, env)
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

    // FROM PARTSUP
    val partsuppData = env.readCsvFile[PARTSUPP](params("dataPath")+TPC_H_utils.TPC_PARTSUPP_TABLE, fieldDelimiter = "|")

    // FROM SUPPLIER
    val supplierData = env.readCsvFile[SUPPLIER](params("dataPath")+TPC_H_utils.TPC_SUPPLIER_TABLE, fieldDelimiter = "|")

    // FROM NATION
    val nationData = env.readCsvFile[NATION](params("dataPath")+TPC_H_utils.TPC_NATION_TABLE, fieldDelimiter = "|")

    tableEnv.createTemporaryView("PARTSUPP", partsuppData)
    tableEnv.createTemporaryView("SUPPLIER", supplierData)
    tableEnv.createTemporaryView("NATION", nationData)

    //val linitemTable = tableEnv.from("LINITEM")

    val result = tableEnv.sqlQuery(
      """
        | SELECT PS_PARTKEY, SUM(PS_SUPPLYCOST*PS_AVAILQTY) AS SUM_V
        | FROM
        | PARTSUPP,
        | SUPPLIER,
        | NATION
        | WHERE
        | PS_SUPPKEY = S_SUPPKEY
        | AND S_NATIONKEY = N_NATIONKEY
        | AND N_NAME = 'GERMANY'
        | GROUP BY
        | PS_PARTKEY
        | HAVING
        | SUM(PS_SUPPLYCOST*PS_AVAILQTY) > (
        | SELECT
        | SUM(PS_SUPPLYCOST*PS_AVAILQTY) * 0.0001000000
        | FROM
        | PARTSUPP,
        | SUPPLIER,
        | NATION
        | WHERE
        | PS_SUPPKEY = S_SUPPKEY
        | AND S_NATIONKEY = N_NATIONKEY AND
        | N_NAME = 'GERMANY'
        | )
        | ORDER BY SUM_V DESC
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

    PlaygroundTools.saveExecutionPlan("TPC_H_Q11-SQL", execPlan, execTime, params, env)
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
