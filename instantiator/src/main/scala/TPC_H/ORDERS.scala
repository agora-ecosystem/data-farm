package TPC_H

import java.sql.Date

/**
  * Created by researchuser7 on 2020-03-05.
  */
case class ORDERS(
                   O_ORDERKEY:Int,
                   O_CUSTKEY:Int,
                   O_ORDERSTATUS:String,
                   O_TOTALPRICE:Float,
                   O_ORDERDATE:Date,
                   O_ORDERPRIORITY:String,
                   O_CLERK:String,
                   O_SHIPPRIORITY:Int,
                   O_COMMENT:String
                 )
