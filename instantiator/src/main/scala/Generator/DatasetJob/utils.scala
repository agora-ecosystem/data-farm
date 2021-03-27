package Generator.DatasetJob

/**
  * Created by researchuser7 on 2020-04-22.
  */
object utils {
  def getElementBySeed(v:Seq[Any], s:Int): Any  ={
    v(s%v.size)
  }

}
