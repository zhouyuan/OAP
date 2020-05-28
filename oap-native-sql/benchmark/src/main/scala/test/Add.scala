package test

import org.apache.spark.sql.SparkSession

object Add {
  def main(args: Array[String]) {
    val fieldsNum = if (args.length > 0) args(0).toInt else 1
    val spark = SparkSession.builder.appName(s"Add_$fieldsNum").getOrCreate()
    var df = spark.read.format("parquet").load("/tpcds/web_sales")

    fieldsNum match {
      case 1 =>
        df = df.select((df("ws_sold_time_sk")).alias("AddValue"))
      case 2 =>
        df = df.select((df("ws_sold_time_sk") + df("ws_ship_date_sk")).alias("AddValue"))
      case 3 =>
        df = df.select(
          (df("ws_sold_time_sk") + df("ws_ship_date_sk") + df("ws_item_sk")).alias("AddValue"))
      case 4 =>
        df = df.select(
          (df("ws_sold_time_sk") + df("ws_ship_date_sk") + df("ws_item_sk") + df(
            "ws_bill_customer_sk")).alias("AddValue"))
      case 5 =>
        df = df.select(
          (df("ws_sold_time_sk") + df("ws_ship_date_sk") + df("ws_item_sk") + df(
            "ws_bill_customer_sk") + df("ws_bill_cdemo_sk")).alias("AddValue"))
      case 6 =>
        df = df.select(
          (df("ws_sold_time_sk") + df("ws_ship_date_sk") + df("ws_item_sk") + df(
            "ws_bill_customer_sk") + df("ws_bill_cdemo_sk") + df("ws_bill_hdemo_sk"))
            .alias("AddValue"))
      case 7 =>
        df = df.select(
          (df("ws_sold_time_sk") + df("ws_ship_date_sk") + df("ws_item_sk") + df(
            "ws_bill_customer_sk") + df("ws_bill_cdemo_sk") + df("ws_bill_hdemo_sk") + df(
            "ws_bill_addr_sk")).alias("AddValue"))
      case 8 =>
        df = df.select(
          (df("ws_sold_time_sk") + df("ws_ship_date_sk") + df("ws_item_sk") + df(
            "ws_bill_customer_sk") + df("ws_bill_cdemo_sk") + df("ws_bill_hdemo_sk") + df(
            "ws_bill_addr_sk") + df("ws_ship_customer_sk")).alias("AddValue"))
      case 9 =>
        df = df.select(
          (df("ws_sold_time_sk") + df("ws_ship_date_sk") + df("ws_item_sk") + df(
            "ws_bill_customer_sk") + df("ws_bill_cdemo_sk") + df("ws_bill_hdemo_sk") + df(
            "ws_bill_addr_sk") + df("ws_ship_customer_sk") + df("ws_ship_cdemo_sk"))
            .alias("AddValue"))
      case 10 =>
        df = df.select(
          (df("ws_sold_time_sk") + df("ws_ship_date_sk") + df("ws_item_sk") + df(
            "ws_bill_customer_sk") + df("ws_bill_cdemo_sk") + df("ws_bill_hdemo_sk") + df(
            "ws_bill_addr_sk") + df("ws_ship_customer_sk") + df("ws_ship_cdemo_sk") + df(
            "ws_ship_hdemo_sk")).alias("AddValue"))
      case _ =>
        throw new Exception("arg not support")
    }

    //df.write.format("parquet").save("/tpcds_output/web_sales/")
    if (args.length == 2 && args(1) == "show") {
      df.show()
    } else {
      df.foreach(r => {})
    }
    spark.stop()
  }
}
