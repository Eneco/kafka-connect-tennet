package com.eneco.trading.kafka.connect.tennet

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scalaj.http._

class TennetConnectorTest extends FunSuite with Matchers with BeforeAndAfter with StrictLogging {
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
 }
//



