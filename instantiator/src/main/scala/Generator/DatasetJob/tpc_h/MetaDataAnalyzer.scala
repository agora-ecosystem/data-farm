package Generator.DatasetJob.tpc_h

import Generator.DatasetJob.tpc_h.table.LineitemOperatorManager
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

import org.apache.flink.api.scala._

/**
  * Created by researchuser7 on 2020-04-20.
  */

object MetaDataAnalyzer {

  import org.apache.flink.api.scala._;
  import org.apache.flink.api.java.io.DiscardingOutputFormat;
  import org.apache.flink.api.scala.ExecutionEnvironment;
  import org.apache.flink.api.common.operators.Order;
  import org.apache.flink.configuration.Configuration;
  import org.joda.time.format.{DateTimeFormat, DateTimeFormatter};
  import scala.collection.JavaConverters._;
  val dateFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");

  def run(params: Map[String, String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //val env = ExecutionEnvironment.createLocalEnvironment()

    val source_lineitem = env.readCsvFile[(Int, Int, Int, Int, Float, Float, Float, Float, String, String, java.sql.Date, java.sql.Date, java.sql.Date, String, String)](params("dataPath").+("lineitem.tbl"), fieldDelimiter = "|")

    val source_orders = env.readCsvFile[(Int, Int, String, Float, java.sql.Date, String, String, Int, String)](params("dataPath").+("orders.tbl"), fieldDelimiter = "|")

    val source_customer = env.readCsvFile[(Int, String, String, Int, String, Float, String, String)](params("dataPath").+("customer.tbl"), fieldDelimiter = "|");

    val source_partsupp = env.readCsvFile[(Int, Int, Int, Float, String)](params("dataPath").+("partsupp.tbl"), fieldDelimiter = "|");

    val source_part = env.readCsvFile[(Int, String, String, String, String, Int, String, Float, String)](params("dataPath").+("part.tbl"), fieldDelimiter = "|");

    val source_supplier = env.readCsvFile[(Int, String, String, Int, String, Float, String)](params("dataPath").+("supplier.tbl"), fieldDelimiter = "|");

    val source_nation = env.readCsvFile[(Int, String, Int, String)](params("dataPath").+("nation.tbl"), fieldDelimiter = "|");


    println(
      s"""
        |{
        |        "customer": ${source_customer.count()},
        |        "lineitem": ${source_lineitem.count()},
        |        "nation": ${source_nation.count()},
        |        "orders": ${source_orders.count()},
        |        "part": ${source_part.count()},
        |        "partsupp": ${source_partsupp.count()},
        |        "supplier": ${source_supplier.count()}
        |    }
      """.stripMargin)

    //val sink6 = filter5.output(new DiscardingOutputFormat());
    //val execPlan = env.getExecutionPlan();

    source_lineitem.output(new DiscardingOutputFormat())
    source_customer.output(new DiscardingOutputFormat())
    source_nation.output(new DiscardingOutputFormat())
    source_orders.output(new DiscardingOutputFormat())
    source_part.output(new DiscardingOutputFormat())
    source_partsupp.output(new DiscardingOutputFormat())
    source_supplier.output(new DiscardingOutputFormat())

    val execResult = env.execute()
    println(StringContext("Runtime execution: ", " ms").s(execResult.getNetRuntime));
    Some(execResult.getNetRuntime)

  }

  def main(args: Array[String]): Unit = {
    val params = if (args.length.==(1))
      Map("dataPath" -> args(0))
    else
      throw new Exception("Expected arguments: <data_path> ");
    run(params)
  };


}