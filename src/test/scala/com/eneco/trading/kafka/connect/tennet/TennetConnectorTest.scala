package com.eneco.trading.kafka.connect.tennet

import java.util
import java.util.Collections

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.SourceTaskContext
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.xml.NodeSeq
import scalaj.http._

import scala.collection.JavaConverters._
import org.scalatest.mock.MockitoSugar

class TennetConnectorTest extends FunSuite with Matchers with BeforeAndAfter with MockitoSugar {
  test("testing get xml") {
    val response: HttpResponse[String] = Http("http://www.tennet.org/xml/balancedeltaprices/balans-delta_2h.xml").asString
    //println(response.body)
    val imbalance = scala.xml.XML.loadString(response.body)
    println("doing something")
    (imbalance \\ "RECORD").foreach { record =>
      println("   record is")
      println(record \ "SEQUENCE_NUMBER")
      println((record \ "NUMBER").text.toInt)
      println((record \ "SEQUENCE_NUMBER").text.toInt)
      println((record \ "TIME").text)
      println((record \ "UPWARD_DISPATCH").text.toDouble)
      println((record \ "DOWNWARD_DISPATCH").text.toDouble)
      println((record \ "RESERVE_UPWARD_DISPATCH").text.toDouble)
      println((record \ "RESERVE_DOWNWARD_DISPATCH").text.toDouble)
      println((record \ "EMERGENCY_POWER").text.toDouble)
    }
    println("next")
    //val results = TennetSourceRecordProducer().produce{"test"}
    //results.map (r =>println(r))
  }
  test ("parse config") {
    println("Start Tennet Connector with: ")

    val props = Map (
      "connector.class" -> "com.eneco.trading.kafka.connect.tennet.TennetSourceConnector",
      "url" -> "http://www.tennet.org/xml/",
      "tasks.max" -> "1",
      "interval"-> "10000",
      "tennet.imbalance.topic"-> "tennet_imbalance",
      "tennet.settlement.prices.topic" -> "tennet_settlementprice",
      "tennet.bidladder.topic" -> "tennet_bidladder"
    )
    println(props.asJava.toString())
    val sourceConfig = new TennetSourceConfig(props.asJava)
    println(sourceConfig.toString)
  }
 }
//



