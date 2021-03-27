package Generator.DatasetJob.imdb_kaggle

/**
  * Created by researchuser7 on 2020-04-20.
  */

object MetaDataAnalyzer {

  import org.apache.flink.api.java.io.DiscardingOutputFormat
  import org.apache.flink.api.scala.{ExecutionEnvironment, _}
  import org.joda.time.format.{DateTimeFormat, DateTimeFormatter};
  val dateFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");

  def run(params: Map[String, String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //val env = ExecutionEnvironment.createLocalEnvironment()

    val source_0 = env.readCsvFile[(String, Int, String, String, String, String)](params("dataPath").+("title.principals.csv"), fieldDelimiter = ";")

    val source_1 = env.readCsvFile[(String, Int, String, String, String, String, String, String)](params("dataPath").+("title.akas.csv"), fieldDelimiter = ";")

    val source_2 = env.readCsvFile[(String, Float, Int)](params("dataPath").+("title.ratings.csv"), fieldDelimiter = ";")

    val source_3 = env.readCsvFile[(String, String, String, String, Int, Int, Int, Int, String)](params("dataPath").+("title.basics.csv"), fieldDelimiter = ";")

    val source_4 = env.readCsvFile[(String, String, Int, Int, String, String)](params("dataPath").+("name.basics.csv"), fieldDelimiter = ";")


//    println(s"== 0 : ${source_3.filter(x => x._5 == 0 ).count()}")
//    println(s"== 1 : ${source_3.filter(x => x._5 == 1 ).count()}")

    // println(s"contains actor : ${source_4.filter(x => x._5 contains "actor" ).count()}")


    println(source_2
      .map(x => x.copy(_2=x._2.toInt))
      .groupBy(1).reduceGroup{v => v.reduce((x1,x2) => x1)}.count())

//    println(
//      s"""
//        |{
//        |        "title.principals": ${source_0.count()},
//        |        "title.akas": ${source_1.count()},
//        |        "title.ratings": ${source_2.count()},
//        |        "title.basics": ${source_3.count()},
//        |        "name.basics": ${source_4.count()}
//        |    }
//      """.stripMargin)

    //val sink6 = filter5.output(new DiscardingOutputFormat());
    //val execPlan = env.getExecutionPlan();

    source_0.output(new DiscardingOutputFormat())
    source_1.output(new DiscardingOutputFormat())
    source_2.output(new DiscardingOutputFormat())
    source_3.output(new DiscardingOutputFormat())
    source_4.output(new DiscardingOutputFormat())

    val execResult = env.execute()
    println(StringContext("Runtime execution: ", " ms").s(execResult.getNetRuntime))
    Some(execResult.getNetRuntime)

  }

  def main(args: Array[String]): Unit = {
    val params = if (args.length.==(1))
      Map("dataPath" -> args(0))
    else
      throw new Exception("Expected arguments: <data_path> ")
    run(params)
  }


}