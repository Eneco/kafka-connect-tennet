package com.eneco.trading.kafka.connect.tennet

/**
  * Created by andrew@datamountaineer.com on 06/02/2017. 
  * kafka-connect-tennet
  */

import java.time.{Duration, Instant, Clock}


class ExponentialBackOff(step: Duration, cap: Duration, iteration: Int = 0, clock: Clock = Clock.systemUTC()) {
  val endTime: Instant = Instant.now(clock).plus(exponentialInterval(iteration))

  def remaining: Duration = Duration.between(Instant.now(clock), endTime)

  def passed: Boolean = Instant.now(clock).isAfter(this.endTime)

  def nextSuccess(): ExponentialBackOff = new ExponentialBackOff(step, cap, 0, clock)

  def nextFailure(): ExponentialBackOff = new ExponentialBackOff(step, cap, iteration + 1, clock)

  private def exponentialInterval(i: Int) = Duration.ofMillis(
    Math.min(
      cap.toMillis,
      step.toMillis * Math.pow(2, i).toLong
    )
  )
}
