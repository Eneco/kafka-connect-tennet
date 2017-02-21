package com.eneco.trading.kafka.connect.tennet

import java.time.{Duration, LocalDate}
import SourceName._

import scala.xml.NodeSeq
import scalaj.http.Http

object TennetHelper {

  def NodeSeqToDouble(value: NodeSeq): Option[Double] = if (value.text.nonEmpty) Some(value.text.toDouble) else None

  def getXml(url: String): String = Http(url).asString.body

  def createPrevDaysList(days : Int) =   List.tabulate(days)(n => LocalDate.now.plusDays(-(n+1)))

  def createNextDaysList(days : Int) =   List.tabulate(days)(n => LocalDate.now.plusDays((n)))
}





