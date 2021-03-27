package TPC_H

/**
  * Created by researchuser7 on 2020-03-05.
  */
case class PARTSUPP(
                     PS_PARTKEY: Int,
                     PS_SUPPKEY: Int,
                     PS_AVAILQTY: Int,
                     PS_SUPPLYCOST: Float,
                     PS_COMMENT: String
                   )

object PARTSUPP{
  val TABLE_SOURCE = "partsupp.tbl"
}
