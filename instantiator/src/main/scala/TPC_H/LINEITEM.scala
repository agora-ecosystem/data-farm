package TPC_H

import java.sql.Date


/**
  * Created by researchuser7 on 2020-03-04.
  */

case class LINEITEM(
                    L_ORDERKEY:Int, // 0
                    L_PARTKEY:Int, // 1
                    L_SUPPKEY:Int, // 2
                    L_LINENUMBER:Int, // 3
                    L_QUANTITY:Float, // 4
                    L_EXTENDEDPRICE:Float, // 5
                    L_DISCOUNT:Float, // 6
                    L_TAX:Float, // 7
                    L_RETURNFLAG:String, // 8
                    L_LINESTATUS:String, // 9
                    L_SHIPDATE:Date, // 10
                    L_COMMITDATE:Date, // 11
                    L_RECEIPTDATE:Date,  // 12
                    L_SHIPINSTRUCT:String, // 13
                    L_SHIPMODE:String, // 14
                    L_COMMENT:String // 16
                  )

object LINEITEM{
  val TABLE_SOURCE = "lineitem.tbl"
}