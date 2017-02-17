package com.eneco.trading.kafka.connect.tennet

import scala.xml.NodeSeq
import scalaj.http.Http


object TennetHelper {
  def NodeSeqToDouble(value: NodeSeq): Option[Double] = if (value.text.nonEmpty) Some(value.text.toDouble) else None

  def getXml(url: String): String = Http(url).asString.body
}
