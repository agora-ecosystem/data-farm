package Generator.DatasetJob.tpc_h.table

import Generator.DatasetJob.{AbstractDatasetOperatorManager, AbstractTableOperatorManager}

import scala.collection.mutable
import scala.util.Random

/**
  * Created by researchuser7 on 2020-04-20.
  */


object TPCHDatasetOperatorManager extends AbstractDatasetOperatorManager {

  val tables: Seq[String] = Seq(
    "lineitem",
    "orders",
    "customer",
    "partsupp",
    "part",
    "supplier"//,
    //"nation"
  )

  def getTableOperatorManager(s: String): AbstractTableOperatorManager = s match {
    case "lineitem" => LineitemOperatorManager()
    case "orders" => OrdersOperatorManager()
    case "customer" => CustomerOperatorManager()
    case "partsupp" => PartsuppOperatorManager()
    case "part" => PartOperatorManager()
    case "supplier" => SupplierOperatorManager()
    case "nation" => NationOperatorManager()
    case _ => throw new Exception(s"Can not find table operator manager for table '$s' ")
  }

  def main(args: Array[String]): Unit = {
    for (i <- 0 to 100){
      try{
        val (tableSequence, joinSequence) = getSourceTableSequence(5, i)
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
