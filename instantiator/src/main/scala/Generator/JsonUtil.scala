package Generator

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import java.io.File

/**
  * Created by researchuser7 on 2020-04-24.
  */
object JsonUtil {
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def toJson(value: Map[Symbol, Any]): String = {
    toJson(value map { case (k, v) => k.name -> v })
  }

  def toJson(value: Any): String = {
    mapper.writeValueAsString(value)
  }

  def toMap[V](json: String)(implicit m: Manifest[V]) = fromJson[Map[String, V]](json)

  def fromJson[T](json: String)(implicit m: Manifest[T]): T = {
    mapper.readValue[T](json)
  }

  def getListOfJsonFilePaths(dir: String):List[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(x=>
        x.isFile && x.getName.endsWith(".json")
      ).sortBy(x=>
        x.getName.replace(".json", "").split("_").last.toInt
      ).map(_.getAbsolutePath).toList
    } else {
      List[String]()
    }
  }


  def main(args: Array[String]): Unit = {
    println(getListOfJsonFilePaths("/Users/researchuser7/IdeaProjects/flink-playground-jobs/out/Experiment6/generated_abstract_exec_plans"))
  }
}
