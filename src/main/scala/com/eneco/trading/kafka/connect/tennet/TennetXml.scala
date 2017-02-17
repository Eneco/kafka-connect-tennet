package com.eneco.trading.kafka.connect.tennet

import scala.xml.NodeSeq

/**
  * Created by dudebowski on 15-2-17.
  */
trait TennetXml {
  def isProcessed(record: Record) : Boolean

  //def NodeSeqToDouble(value: NodeSeq) : Option[Double] = if (value.text.nonEmpty) Some(value.text.toDouble) else None

  def fromBody(): Seq[Record]

}




abstract class Record

