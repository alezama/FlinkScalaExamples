package com.karanu.streams

import java.util.Properties

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.twitter.TwitterSource

import scala.util.Properties.envOrNone

/**
  * Flink program that filter tweets  showing only those  from English language
  * The filter make use of the language tag received from the JSON Twitter representation
  */
object FilterEnglishTweets {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    // Properties to establish the connection wiht the Twitter API

    properties.setProperty(TwitterSource.CONSUMER_KEY, envOrNone("CONSUMER_KEY").get)
    properties.setProperty(TwitterSource.CONSUMER_SECRET, envOrNone("CONSUMER_SECRET").get)
    properties.setProperty(TwitterSource.TOKEN, envOrNone("TOKEN").get)
    properties.setProperty(TwitterSource.TOKEN_SECRET, envOrNone("TOKEN_SECRET").get)

    env.addSource(new TwitterSource(properties))
      // map JSON representation of a tweet into a Tweet object
      .map(s=> new TwitterMapper().map(s):Tweet)
      // Show only the English tweets
      .filter(_.language.equalsIgnoreCase("en"))
      .print()

    env.execute("Twitter Stream")



  }





}
