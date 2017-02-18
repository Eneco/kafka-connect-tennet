package com.eneco.trading.kafka.connect.tennet

import scala.xml.NodeSeq
import scalaj.http.Http


object TennetHelper {
  def NodeSeqToDouble(value: NodeSeq): Option[Double] = if (value.text.nonEmpty) Some(value.text.toDouble) else None

  def getXml(url: String): String = {
    if (url.contains("localhost")) xmlString
    else Http(url).asString.body
  }

  //TODO move to emebedded http server for testing
  val xmlString =
    """
        <BALANCE_DELTA>
        <RECORD>
        <NUMBER>1</NUMBER>
        <SEQUENCE_NUMBER>962</SEQUENCE_NUMBER>
        <TIME>16:01</TIME>
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
        <NUMBER>2</NUMBER>
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
}


