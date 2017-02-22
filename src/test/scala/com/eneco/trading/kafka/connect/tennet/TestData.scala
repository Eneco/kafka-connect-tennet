package com.eneco.trading.kafka.connect.tennet

import java.time.ZoneId
import java.util

import org.apache.kafka.connect.storage.OffsetStorageReader

import scala.collection.JavaConverters._
import scala.xml.NodeSeq

/**
  * Created by dudebowski on 15-2-17.
  */
class TestData {

}

object TestData {

  val xmlString =
    """
        <BALANCE_DELTA>
        <RECORD>
        <IGCCCONTRIBUTION_DOWN>0</IGCCCONTRIBUTION_DOWN>
        <UPWARD_DISPATCH>0</UPWARD_DISPATCH>
        <DOWNWARD_DISPATCH>0</DOWNWARD_DISPATCH>
        <RESERVE_UPWARD_DISPATCH>0</RESERVE_UPWARD_DISPATCH>
        <RESERVE_DOWNWARD_DISPATCH>0</RESERVE_DOWNWARD_DISPATCH>
        <INCIDENT_RESERVE_UP_INDICATOR>0</INCIDENT_RESERVE_UP_INDICATOR>
        <INCIDENT_RESERVE_DOWN_INDICATOR>0</INCIDENT_RESERVE_DOWN_INDICATOR>
        <MID_PRICE>35.14</MID_PRICE>
        </RECORD>
        <RECORD>
        <NUMBER>1</NUMBER>
        <SEQUENCE_NUMBER>963</SEQUENCE_NUMBER>
        <TIME>16:02</TIME>
        <IGCCCONTRIBUTION_UP>309</IGCCCONTRIBUTION_UP>
        <IGCCCONTRIBUTION_DOWN>0</IGCCCONTRIBUTION_DOWN>
        <UPWARD_DISPATCH>0</UPWARD_DISPATCH>
        <DOWNWARD_DISPATCH>0</DOWNWARD_DISPATCH>
        <RESERVE_UPWARD_DISPATCH>0</RESERVE_UPWARD_DISPATCH>
        <RESERVE_DOWNWARD_DISPATCH>0</RESERVE_DOWNWARD_DISPATCH>
        <INCIDENT_RESERVE_UP_INDICATOR>0</INCIDENT_RESERVE_UP_INDICATOR>
        <INCIDENT_RESERVE_DOWN_INDICATOR>0</INCIDENT_RESERVE_DOWN_INDICATOR>
        <MID_PRICE>35.14</MID_PRICE>
        </RECORD>
        </BALANCE_DELTA>
"""
  val bidLadderRecordXmlString =
    """
    <BIDLADDER>
    <RECORD>
    <IGCCCONTRIBUTION_DOWN>0</IGCCCONTRIBUTION_DOWN>
    <UPWARD_DISPATCH>0</UPWARD_DISPATCH>
    <DOWNWARD_DISPATCH>0</DOWNWARD_DISPATCH>
    <RESERVE_UPWARD_DISPATCH>0</RESERVE_UPWARD_DISPATCH>
    <RESERVE_DOWNWARD_DISPATCH>0</RESERVE_DOWNWARD_DISPATCH>
    <INCIDENT_RESERVE_UP_INDICATOR>0</INCIDENT_RESERVE_UP_INDICATOR>
    <INCIDENT_RESERVE_DOWN_INDICATOR>0</INCIDENT_RESERVE_DOWN_INDICATOR>
    <MID_PRICE>35.14</MID_PRICE>
    </RECORD>
    <BIDLADDER>
    """

  val balanceDeltaSourceType = SourceType(SourceName.BALANCE_DELTA_NAME.toString, "topic","http://localhost:8899/testdata/",ZoneId.of("Europe/Amsterdam"), 0,0)

  def bidLadderRecord: NodeSeq = {
    scala.xml.XML.loadString(bidLadderRecordXmlString) \\ "Record"
  }

  def connectConfiguration: TennetSourceConfig = {
   val props = Map(
      "connector.class" -> "com.eneco.trading.kafka.connect.tennet.TennetSourceConnector",
      "tennet.url" -> "http://localhost:8899/testdata/",
      "tasks.max" -> "1",
      "interval" -> "10000",
      "tennet.balance.delta.topic" -> "tennet_imbalancedelta",
      "tennet.imbalance.topic" -> "tennet_settlementprice",
      "tennet.bid.ladder.topic" -> "tennet_bidladder",
      "tennet.bid.ladder.total.topic" -> "tennet_bidladdertotal",
      "tennet.price.ladder.topic" -> "tennet_priceladder"

    )
    new TennetSourceConfig(props.asJava)
  }
}

class MockOffsetStorageReader extends OffsetStorageReader {
  override def offset[T](partition: util.Map[String, T]): util.Map[String, AnyRef] = {
    null
  }

  override def offsets[T](partitions: util.Collection[util.Map[String, T]]): util.Map[util.Map[String, T], util.Map[String, AnyRef]] = {
    null
  }
}



