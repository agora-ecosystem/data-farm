package Generator
import java.nio.file.Paths

import scala.reflect.runtime.universe._


/**
  * Created by researchuser7 on 2020-03-17.
  */
object JobGeneratorHelper {

  val LINEITME = "lineitem.tbl"
  def getInputDataPath(workingDir: String) = Paths.get(workingDir, LINEITME).toString

  def saveToFile(path: String, code: Tree) = {
    val writer = new java.io.PrintWriter(path)
    try writer.write(showCode(code))
    finally writer.close()
  }

}
