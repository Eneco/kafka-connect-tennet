package com.eneco.trading.kafka.connect.tennet

object TennetSourceType extends Enumeration {
  type TennetSourceType = Value
  val BalanceDelta = Value("balans-delta")
  val Bidladder = Value("laddersize15")
  val BidladderTotal = Value("laddersizetotal")
  val ImbalancePrice  = Value("imbalanceprice")
}
