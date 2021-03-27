package Generator.DatasetJob

import Generator.DatasetHelper
import Generator.DatasetJob.imdb_kaggle.table.IMDBKDatasetOperatorManager
import Generator.DatasetJob.tpc_h.table.TPCHDatasetOperatorManager
import Generator.DatasetJob.utils.getElementBySeed

import scala.collection.mutable
import scala.util.Random

/**
  * Created by researchuser7 on 2020-04-20.
  */

case class JoinRelation(lx: AbstractTableOperatorManager, rx: AbstractTableOperatorManager, field: String) {
  override def equals(joinRelation: Any): Boolean = {
    joinRelation match {
      case jr: JoinRelation => ((rx == jr.rx) && (lx == jr.lx) && (field == jr.field)) || ((rx == jr.lx) && (lx == jr.rx) && (field == jr.field))
      case _ => false
    }
  }

  override def toString: String = f"$lx%10s - $rx%10s : $field%10s "
}

trait AbstractDatasetOperatorManager {

  val tables: Seq[String]

  def getTableOperatorManager(s: String): AbstractTableOperatorManager


  val maxAttempts = 100
  val random: Random.type = Random

  private def getNextJoinRelation(t0_OpMan: AbstractTableOperatorManager): JoinRelation = {

    // Get random join field
    var t0_jField = getElementBySeed(t0_OpMan.joinFields, this.random.nextInt(20)).toString

    // Get random join table
    var t0_jTable = getElementBySeed(t0_OpMan.joinFieldTable(t0_jField).keys.toSeq, this.random.nextInt(20)).toString

    //Get join table field
    var t0_jTableField = t0_OpMan.joinFieldTable(t0_jField)(t0_jTable)

    var t1_OpMan = getTableOperatorManager(t0_jTable)

    JoinRelation(t0_OpMan, t1_OpMan, t0_jTableField)
  }

  def getSourceTableSequence(nJoins: Int, seed: Int = 0): (Seq[AbstractTableOperatorManager], Seq[JoinRelation]) = {
    var success = false
    var tmp_seed = seed
    var attempt = 0
    var tS:Seq[AbstractTableOperatorManager]=null
    var jS:Seq[JoinRelation]=null

    while (!success && (attempt <= 10)) {
      try {
        val (resTS, resJS) = _getSourceTableSequence(nJoins, tmp_seed)
        tS = resTS
        jS = resJS
        success = true
      } catch {
        case e: Exception =>
          attempt+=1
          success = false
          tmp_seed += 1
      }
    }
    (tS, jS)
  }

  def _getSourceTableSequence(nJoins: Int, seed: Int = 0): (Seq[AbstractTableOperatorManager], Seq[JoinRelation]) = {

    this.random.setSeed(seed)

    var tableSequence: mutable.Seq[AbstractTableOperatorManager] = mutable.Seq()
    var joinSequence: mutable.Seq[JoinRelation] = mutable.Seq()

    // Init table
    var t0 = getElementBySeed(tables, seed).toString
    var t0_OpMan = getTableOperatorManager(t0)

    tableSequence = tableSequence :+ t0_OpMan

    for (i <- 0 until nJoins) {
      var nextJoinRelation: JoinRelation = null
      var attempt = 0

      do {
        attempt += 1

        nextJoinRelation = getNextJoinRelation(t0_OpMan)
      } while (joinSequence.contains(nextJoinRelation) && (attempt < maxAttempts))

      if (attempt >= maxAttempts) {
        throw new Exception(s"Impossible to generate '${nJoins}' join sequence for ${tableSequence}.")
      }

      joinSequence = joinSequence :+ nextJoinRelation
      t0_OpMan = nextJoinRelation.rx
      tableSequence = tableSequence :+ t0_OpMan
    }

    (tableSequence, joinSequence)
  }

}

object AbstractDatasetOperatorManager {
  def apply(s: String): AbstractDatasetOperatorManager = {
    if (s == "TPC_H")
      TPCHDatasetOperatorManager
    else if (s == "IMDBK")
      IMDBKDatasetOperatorManager
    else
      throw new Exception(s"Can not find Dataset Operator Manger '$s'.")
  }
}
