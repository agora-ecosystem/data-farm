package Generator

object JobTemplate {

  import org.apache.flink.api.scala._
  import org.apache.flink.api.java.io.DiscardingOutputFormat
  import org.apache.flink.api.scala.ExecutionEnvironment

  import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

  val dateFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")

  def run(execute:Boolean=true): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment

  }

  def main(args: Array[String]): Unit = {
    run()
  }

}
