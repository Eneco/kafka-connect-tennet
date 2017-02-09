package com.eneco.trading.kafka.connect.tennet

import java.time.Duration

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.errors.ConnectException
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.collection.JavaConverters._
import scala.util.{Failure, Try}
import scalaj.http._

class TennetConnectorTest extends FunSuite with Matchers with BeforeAndAfter with StrictLogging {
  test("testing get xml") {
    val response: HttpResponse[String] = Http("http://www.tennet.org/xml/balancedeltaprices/balans-delta_2h.xml").asString
    //println(response.body)
    val imbalance = scala.xml.XML.loadString(response.body)
    println("doing something")
    (imbalance \\ "RECORD").foreach { record =>
//      println("   record is")
//      println(record \ "SEQUENCE_NUMBER")
//      println((record \ "NUMBER").text.toInt)
//      println((record \ "SEQUENCE_NUMBER").text.toInt)
//      println((record \ "TIME").text)
//      println((record \ "UPWARD_DISPATCH").text.toDouble)
//      println((record \ "DOWNWARD_DISPATCH").text.toDouble)
//      println((record \ "RESERVE_UPWARD_DISPATCH").text.toDouble)
//      println((record \ "RESERVE_DOWNWARD_DISPATCH").text.toDouble)
//      println((record \ "EMERGENCY_POWER").text.toDouble)
    }
    println("next")

  }
  test ("parse config") {
    println("parsing config: ")
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
  }

  test ("initiate backoff") {
    println("Start Tennet Connector with: ")
    val interval  = Duration.parse("PT10S")
    val maxBackOff = Duration.parse("PT40M")
    var backoff = new ExponentialBackOff(interval, maxBackOff)
    println(backoff.endTime)

  }

}



