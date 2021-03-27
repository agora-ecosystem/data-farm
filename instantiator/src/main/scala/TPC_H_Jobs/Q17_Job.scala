package TPC_H_Jobs

import java.text.SimpleDateFormat

import PlaygroundUtils.PlaygroundTools
import TPC_H.{CUSTOMER, LINEITEM, ORDERS, PART, TPC_H_utils}
import org.apache.flink.api.common.functions.RichFilterFunction
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
  * SELECT
  * SUM(L_EXTENDEDPRICE)/7.0 AS AVG_YEARLY
  * FROM
  * LINEITEM,
  * PART
  * WHERE
  * P_PARTKEY = L_PARTKEY
  * AND P_BRAND = 'Brand#23'
  * AND P_CONTAINER = 'MED BOX'
  * AND L_QUANTITY < (
  *
  * SELECT
  *       0.2*AVG(L_QUANTITY)
  * FROM
  * LINEITEM
  * WHERE
  * L_PARTKEY = P_PARTKEY
  *
  * )
  *
  */
object Q17_Job {

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

    // FROM PART
    val partData = env.readCsvFile[PART](params("dataPath")+TPC_H_utils.TPC_PART_TABLE, fieldDelimiter = "|")
    val partFiltered = partData.filter(x => (x.P_BRAND == "Brand#23") && (x.P_CONTAINER == "MED BOX"))

    /* Inner query => create a DataSet[(P_PARTKEY, 0.2*AVG(L_QUANTITY))] to be broadcast and to lookup in the external query
     *
     * SELECT
     *   0.2*AVG(L_QUANTITY)
     * FROM
     *   LINEITEM
     * WHERE
     *   L_PARTKEY = P_PARTKEY
     */
    val innerQueryResLookupMap = linitemData.join(partFiltered).where("L_PARTKEY").equalTo("P_PARTKEY") {
      (lx, rx) => (rx.P_PARTKEY, lx.L_QUANTITY, 1)
    }.groupBy(0)
      .reduce((x1, x2) => (x1._1, x1._2 + x2._2, x1._3 + x2._3))
      .map(x => (x._1, 0.2 * x._2 / x._3))

    // WHERE P_PARTKEY = L_PARTKEY
    val l_p_Joined = linitemData.join(partFiltered).where("L_PARTKEY").equalTo("P_PARTKEY") {
      (lx, rx) => (rx.P_PARTKEY, lx.L_QUANTITY, lx.L_EXTENDEDPRICE)
    }


    // WHERE L_QUANTITY < (...)
    val l_p_Filtered = l_p_Joined.filter(new RichFilterFunction[(Int, Float, Float)] {

      var broadcastSet: Map[Int, Double] = _

      override def open(config: Configuration): Unit = {
        // 3. Access the broadcast DataSet as a Collection
        broadcastSet = getRuntimeContext.getBroadcastVariable[(Int, Double)]("innerQueryResLookupMap").asScala.toMap
      }

      override def filter(x: (Int, Float, Float)): Boolean = {
        val opt = broadcastSet.get(x._1)

        if (opt.isDefined) x._2 < opt.get else false
      }
    }).withBroadcastSet(innerQueryResLookupMap, "innerQueryResLookupMap")

    //l_p_Filtered.first(20).print()

    val queryResult = l_p_Filtered.map(_._3).reduce((x1, x2) => x1 + x2).map(x => (0, x / 7.0))

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

    PlaygroundTools.saveExecutionPlan("TPC_H_Q17", execPlan, execTime, params, env)

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

    // FROM PART
    val partData = env.readCsvFile[PART](params("dataPath")+TPC_H_utils.TPC_PART_TABLE, fieldDelimiter = "|")

    tableEnv.createTemporaryView("LINEITEM", linitemData)
    tableEnv.createTemporaryView("PART", partData)

    val result = tableEnv.sqlQuery(
      """
        | SELECT
        | SUM(L_EXTENDEDPRICE)/7.0 AS AVG_YEARLY
        | FROM
        | LINEITEM,
        | PART
        | WHERE
        | P_PARTKEY = L_PARTKEY
        | AND P_BRAND = 'Brand#23'
        | AND P_CONTAINER = 'MED BOX'
        | AND L_QUANTITY < (
        |
        | SELECT
        |       0.2*AVG(L_QUANTITY)
        | FROM
        | LINEITEM
        | WHERE
        | L_PARTKEY = P_PARTKEY
        |
        | )
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

    PlaygroundTools.saveExecutionPlan("TPC_H_Q17-SQL", execPlan, execTime, params, env)
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
