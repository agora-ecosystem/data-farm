package TPC_H

import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

/**
  * Created by researchuser7 on 2020-03-05.
  */
object TPC_H_utils {

  val TPC_LINITEM_TABLE: String = "lineitem.tbl"

  val TPC_CUSTOMER_TABLE: String = "customer.tbl"

  val TPC_ORDERS_TABLE: String = "orders.tbl"

  val TPC_PART_TABLE: String = "part.tbl"

  val TPC_SUPPLIER_TABLE: String = "supplier.tbl"

  val TPC_NATION_TABLE: String = "nation.tbl"

  val TPC_REGION_TABLE: String = "region.tbl"

  val TPC_PARTSUPP_TABLE: String = "partsupp.tbl"

  val formatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")

}
