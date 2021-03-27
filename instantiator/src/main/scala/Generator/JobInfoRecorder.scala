package Generator

import java.io.FileWriter
import java.nio.file.Paths

import Generator.DatasetJob.{AbstractTableOperatorManager, JoinRelation}

import scala.collection.mutable

/**
  * Created by researchuser7 on 2020-04-23.
  */


object JobInfoRecorder {

  case class DataInfoRecord(
                             id: Int,
                             varName: String,
                             pact: String,
                             tables: Seq[String],
                             selectivity: Option[Double] = None,
                             outCardinality: Option[Int] = None,
                             complexity: Option[Int] = None
                           ) {
    override def toString: String = f"$id%3d : $pact%15s : $varName%8s -> s = ${selectivity.getOrElse("None")}%5s c = ${outCardinality.getOrElse("None")}%4s | Tables: ${tables.mkString(", ")}"

    def toJson:String ={
      JsonUtil.toJson(this)
    }

  }

  case class JobDataInfoRecorder(JobId: String) {

    val operatorsInfo: mutable.Queue[DataInfoRecord] = mutable.Queue()
    var tableSequence: Option[Seq[AbstractTableOperatorManager]] = None
    var joinRelationsSequence: Option[Seq[JoinRelation]] = None

    def recordTableSequence(tableSequence: Seq[AbstractTableOperatorManager]): Unit = {
      this.tableSequence = Some(tableSequence)
    }

    def recordJoinRelationsSequence(joinRelationsSequence: Seq[JoinRelation]): Unit = {
      this.joinRelationsSequence = Some(joinRelationsSequence)
    }

    def record(id: Int, varName: String, pact: String, tables: Seq[String], selectivity: Option[Double] = None, outCardinality: Option[Int] = None, complexity: Option[Int] = None) {
      operatorsInfo.enqueue(DataInfoRecord(id, varName, pact, tables, selectivity, outCardinality, complexity))
    }

    override def toString: String = f"----------\n$JobId%10s\n----------\n" + operatorsInfo.mkString("\n")

    def toJson:String ={
      JsonUtil.toJson(this)
    }

  }

  var currentJobInfo: Option[JobDataInfoRecorder] = None

  val jobsInfo: mutable.Queue[JobDataInfoRecorder] = mutable.Queue()

  private def apply(JobId: String): JobDataInfoRecorder = JobDataInfoRecorder(JobId)

  def openJobRecorder(JobId: String): JobDataInfoRecorder = {
    currentJobInfo = Some(JobDataInfoRecorder(JobId))
    currentJobInfo.get
  }

  def closeJobRecorder(): Boolean = {
    if (currentJobInfo.isDefined) {
      jobsInfo.enqueue(currentJobInfo.get)
      currentJobInfo = None
      true
    } else {
      false
    }
  }

  override def toString: String = currentJobInfo.getOrElse("None").toString

  def toJson:String ={
    JsonUtil.toJson(this.jobsInfo)
  }

  def persist(path:String): Unit ={
    val file = Paths.get(path, "generated_jobs_info.json").toFile
    val fw = new FileWriter(file)
    fw.write(this.toJson)
    fw.close()

  }

}
