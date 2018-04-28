package com.karanu.streams

import java.util.{Date, Properties}

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.twitter.TwitterSource
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
import scala.util.Properties._

/**
  * Get the top hashtag of a twitter connection
  */
object TopHashMap {

  def main(args: Array[String]): Unit = {

    // The window procesing size
    val WINDOW_SIZE_MIN = 1

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Connection parameters to the Twitter API
    val properties = new Properties()
    properties.setProperty(TwitterSource.CONSUMER_KEY, envOrNone("CONSUMER_KEY").get)
    properties.setProperty(TwitterSource.CONSUMER_SECRET, envOrNone("CONSUMER_SECRET").get)
    properties.setProperty(TwitterSource.TOKEN, envOrNone("TOKEN").get)
    properties.setProperty(TwitterSource.TOKEN_SECRET, envOrNone("TOKEN_SECRET").get)

    env.addSource(new TwitterSource(properties))
      // Map the stream of tweets in JSON format to Tweet objects
      .map(s => {
        new TwitterMapper().map(s): Tweet
      }).flatMap((tweet: Tweet, coll: Collector[(String, Int)]) =>
      for (tag: String <- tweet.tags) coll.collect(tag, 1))
      // keyed the stream by tag
      .keyBy(0)
      // tumbling time window of 1 minute length
      .timeWindow(Time.minutes(WINDOW_SIZE_MIN))
      // compute the sum over the number of tags
      .sum(1)
      // performs the operation with single parallelism
      .timeWindowAll(Time.minutes(WINDOW_SIZE_MIN))
      // We get the hastag with the maximum number of mentions
      .apply((tw: TimeWindow, it: Iterable[(String, Int)], col: Collector[(Date,String,Int)]) => {
        var topTag: String = null
        var count = 0
        for (tuple: (String, Int) <- it) {
          if (tuple._2 > count) {
            topTag = tuple._1
            count = tuple._2
          }
        }

        // The exit value contains the window's start time, the top tag and the count
        col.collect((new Date(tw.getStart), topTag, count))

      })
      .print()



    env.execute()


  }
}