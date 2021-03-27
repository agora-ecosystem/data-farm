package Generator.DatasetJob.imdb_kaggle.table

import Generator.DatasetJob.{AbstractDatasetOperatorManager, AbstractTableOperatorManager}

/**
  * Created by researchuser7 on 2020-04-20.
  */


object IMDBKDatasetOperatorManager extends AbstractDatasetOperatorManager {

  val tables: Seq[String] = Seq(
    "title.akas",
    "title.basics",
    "name.basics",
    "title.ratings",
    "title.principals"
  )

  def getTableOperatorManager(s: String): AbstractTableOperatorManager = s match {
    case "name.basics" => NameBasicsOperatorManager()
    case "title.akas" => TitleAkasOperatorManager()
    case "title.basics" => TitleBasicsOperatorManager()
    case "title.principals" => TitlePrincipalsOperatorManager()
    case "title.ratings" => TitleRatingsOperatorManager()
    case _ => throw new Exception(s"Can not find table operator manager for table '$s' ")
  }

  def main(args: Array[String]): Unit = {
    for (i <- 0 to 100){
      try{
        val (tableSequence, joinSequence) = getSourceTableSequence(3, i)
        joinSequence.foreach( j => println(s"> $j"))
        println()
        println(s"Table sequence: $tableSequence")
      } catch {
        case ex:Exception => println(ex)
      } finally {
        println("---------------------------------------------")
      }
    }
  }
}
