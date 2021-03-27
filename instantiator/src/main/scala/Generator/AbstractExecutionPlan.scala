package Generator

import scala.io._
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.jgrapht.GraphPath
import org.jgrapht.alg.shortestpath.DijkstraShortestPath
import org.jgrapht.graph.{DefaultEdge, DirectedWeightedMultigraph}
import org.jgrapht.traverse.DepthFirstIterator

import collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by researchuser7 on 2020-03-17.
  */


case class AbstractExecutionPlan(execPlan: DirectedWeightedMultigraph[AbstractOperator, DefaultEdge]) {

  def this() = {
    this(new DirectedWeightedMultigraph[AbstractOperator, DefaultEdge](classOf[DefaultEdge]))
  }

  def edgeSet() = execPlan.edgeSet().asScala

  def vertexSet() = execPlan.vertexSet().asScala

  def sources() = execPlan.vertexSet().asScala.filter(_.opId.contains("Data Source"))

  def sourcesOf(opId: String) = {
    //val v = this.getVertexFromId(opId)
    this.sources()
      .map(s =>
        (s, this.getBranchFromTo(s.opId, opId))
      )
      .filter(_._2.execPlan.vertexSet().size() > 0)
      .map(_._1)
  }

  def sinks() = execPlan.vertexSet().asScala.filter(_.opId.contains("Data Sink"))

  def getVertexFromId(opId: String) = execPlan.vertexSet().asScala.find(_.opId.equals(opId)).get

  //TODO check if depth traversal is good in case of operatore with multiple outputs
  def depthVisitFrom(opId: String) = {
    val v = getVertexFromId(opId)
    val depthIterator = new DepthFirstIterator[AbstractOperator, DefaultEdge](execPlan, v) //.asScala
    depthIterator.asScala.toArray
  }

  def children(opId: String) = {
    val vs = execPlan.vertexSet().asScala.filter(_.opId == opId)
    vs.flatMap(v => execPlan.outgoingEdgesOf(v).asScala.map(e => execPlan.getEdgeTarget(e)))
  }

  def parents(opId: String) = {
    val vs = execPlan.vertexSet().asScala.filter(_.opId == opId)
    vs.flatMap(v => execPlan.incomingEdgesOf(v).asScala.map(e => execPlan.getEdgeTarget(e)))
  }

  def getBranchFromTo(sourceOpId: String, targetOpId: String) = {
    val sourceV = getVertexFromId(sourceOpId)
    val targetV = getVertexFromId(targetOpId)

    val dsp = new DijkstraShortestPath[AbstractOperator, DefaultEdge](this.execPlan)

    val path = Option(dsp.getPath(sourceV, targetV))

    val subPlan = new AbstractExecutionPlan

    if (path.isDefined){
      for (v <- path.get.getVertexList.asScala) {
        //println(v)
        subPlan.execPlan.addVertex(v)
      }

      for (e <- path.get.getEdgeList.asScala) {
        //println(s"${this.execPlan.getEdgeSource(e)} --> ${this.execPlan.getEdgeTarget(e)}")
        subPlan.execPlan.addEdge(
          this.execPlan.getEdgeSource(e),
          this.execPlan.getEdgeTarget(e))
      }
    }
    subPlan
  }
}

object AbstractExecutionPlan {

  def parseExecPlan(planPath: String): AbstractExecutionPlan = {

    val filename = planPath
    // read
    println(s"Reading $planPath ...")
    val json = Source.fromFile(filename)
    // parse
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val parsedJson = JsonUtil.fromJson[Map[String, Object]](json.getLines().mkString("\n"))
    //println(parsedJson)

    val asbPlan = new AbstractExecutionPlan()

    // Parse nodes
    //println()
    for (n <- parsedJson.get("nodes").asInstanceOf[Some[Seq[Map[String, String]]]].get) {
      //println(n)
      asbPlan.execPlan.addVertex(new AbstractOperator(n("id")))
    }

    //Parse edges
    //println()
    for (e <- parsedJson.get("links").asInstanceOf[Some[Seq[Map[String, String]]]].get) {
      //println(e)
      asbPlan.execPlan.addEdge(new AbstractOperator(e("source")),
        new AbstractOperator(e("target")))
    }

    //Show graphs edges
    //println()
    val edges = asbPlan.execPlan.edgeSet().asScala
    //edges.foreach(e => println(s"${asbPlan.execPlan.getEdgeSource(e)} --> ${asbPlan.execPlan.getEdgeTarget(e)}"))

    asbPlan
  }


  def main(args: Array[String]): Unit = {
    val absPlan = parseExecPlan("/Users/researchuser7/PycharmProjects/flink-execution-plan-generator/out/plan_0.json")

    println()
    val ps = absPlan.parents("Map_1")
    println(ps)

    println()
    for (op <- absPlan.depthVisitFrom(absPlan.sources().toSeq.head.opId)) {
      println(op.opId)
    }

    println()
    for (s <- absPlan.sources()) {
      for (v <- absPlan.depthVisitFrom(s.opId)) {
        println(v.opId)
      }
      println("--------")
    }


    println(absPlan.sourcesOf("Data Sink_6"))


  }

}
