package com.intel.sparkColumnarPlugin

import org.apache.spark.sql.{Row, SparkSession}

import org.scalatest.FunSuite

class ExtensionSuite extends FunSuite {

  private def stop(spark: SparkSession): Unit = {
    spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  test("inject columnar exchange") {
    val session = SparkSession
      .builder()
      .master("local[1]")
      .config("org.apache.spark.example.columnar.enabled", value = true)
      .config("spark.sql.extensions", "com.intel.sparkColumnarPlugin.ColumnarPlugin")
      .appName("inject columnar exchange")
      .getOrCreate()

    try {
      import session.sqlContext.implicits._

      val input = Seq((100), (200), (300))
      val data = input.toDF("vals").repartition(1)
      val result = data.collect()
      assert(result sameElements input.map(x => Row(x)))
    } finally {
      stop(session)
    }
  }
}
