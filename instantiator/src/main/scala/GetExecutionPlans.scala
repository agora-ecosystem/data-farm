import PlaygroundUtils.PlaygroundTools
import TPC_H_Jobs.{Q11_Job, Q13_Job, Q17_Job, Q1_Job, Q21_Job, Q3_Job}

/**
  * Created by researchuser7 on 2020-03-09.
  */
object GetExecutionPlans {

  def main(args: Array[String]): Unit = {


    val params = Map(
      "dataPath" -> args(0),
      "execPlanOutPath" -> args(1),
      "execute" -> (args(2) == "exec").toString,
      "local" -> (args(3) == "local").toString,
      "heap_size" -> args(4)
    )

    Q1_Job.run(params)
    Q1_Job.run_sql(params)

    Q3_Job.run(params)
    Q3_Job.run_sql(params)
//
    Q11_Job.run(params)
    Q11_Job.run_sql(params)
//
    Q13_Job.run(params)
    Q13_Job.run_sql(params)

    Q17_Job.run(params)
    Q17_Job.run_sql(params)

    Q21_Job.run(params)
    Q21_Job.run_sql(params)

  }


}
