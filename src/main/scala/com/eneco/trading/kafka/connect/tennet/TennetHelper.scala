package com.eneco.trading.kafka.connect.tennet

import java.time.{Clock, LocalDate}

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.storage.OffsetStorageReader

import scala.xml.NodeSeq
import scalaj.http.Http

trait ServiceProvider {
  val storageReader: OffsetStorageReader
  val xmlReader: XmlReader
  val clock: Clock
}

trait XmlReader {
  def getXml(url: String): Option[String]
}

object HttpXmlReader extends XmlReader with StrictLogging {
  def getXml(url: String): Option[String] = {
    val response = Http(url).asString

    response.code match {
      case 200 => Some(response.body)
      case code => {
        logger.warn(s"Failed to get records for $url: $code")
        None
      }
    }
  }
}

object TennetHelper extends StrictLogging {

  def NodeSeqToDouble(value: NodeSeq): Option[Double] = if (value.text.nonEmpty) Some(value.text.toDouble) else None

//  def createPrevDaysList(days : Int) =   List.tabulate(days)(n => LocalDate.now.plusDays(-(n+1)))
//
//  def createNextDaysList(days : Int) =   List.tabulate(days)(n => LocalDate.now.plusDays((n)))
}





