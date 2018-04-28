package com.karanu.streams

import java.util.{Date, Properties}

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.twitter.TwitterSource
import org.apache.flink.util.Collector

import scala.util.Properties.envOrNone

object NumberOfTweetsPerLanguage {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    properties.setProperty(TwitterSource.CONSUMER_KEY, envOrNone("CONSUMER_KEY").get)
    properties.setProperty(TwitterSource.CONSUMER_SECRET, envOrNone("CONSUMER_SECRET").get)
    properties.setProperty(TwitterSource.TOKEN, envOrNone("TOKEN").get)
    properties.setProperty(TwitterSource.TOKEN_SECRET, envOrNone("TOKEN_SECRET").get)


    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.addSource(new TwitterSource(properties)).map(
     s=> new TwitterMapper().map(s):Tweet)
      .keyBy( tweet => tweet.language)
      .timeWindow(Time.minutes(1))
      .apply((key:String, twindow:TimeWindow, it:Iterable[Tweet], coll:Collector[(String, Int, Date)]) =>
      {coll.collect((key, it.size, new Date(twindow.getEnd)))}).print()

    env.execute("Tweets count by language")
  }
}
