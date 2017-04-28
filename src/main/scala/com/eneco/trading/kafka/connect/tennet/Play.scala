package com.eneco.trading.kafka.connect.tennet

import org.apache.kafka.connect.data.Struct

/**
  * Created by joshua on 28-4-17.
  */
object Play extends App{
  println("Hello world")

  // this works
  val maxval2 = Seq(1.toLong,2.toLong,3.toLong).max

  // why doesn't this work
  val struct1 = new Struct(TennetSourceConfig.SCHEMA_IMBALANCE).put("value_time", 1.toLong)
  val struct2 = new Struct(TennetSourceConfig.SCHEMA_IMBALANCE).put("value_time", 2.toLong)
  val struct3 = new Struct(TennetSourceConfig.SCHEMA_IMBALANCE).put("value_time", 3.toLong)
  val maxval = Seq(struct3, struct2, struct1).map(_.getInt64("value_time")).max

  // override def mapRecord(record: Node, generatedAt: Long): BalanceDeltaSourceRecord = {

  //def getValueTime(struct : Struct) : Long =

  print(maxval)

  // val newestValueTime = records.maxBy(_.get("value_time"))

  //print(records.max)
  //truncateOffsets(k => k.toLong > newestValueTime - OFFSET_MAX_MILLIS)

}

case class Foo(ValueTime: Long)