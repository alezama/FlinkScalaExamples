package com.karanu.streams

import java.util.Properties

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.twitter.TwitterSource
import org.apache.flink.util.Collector

import scala.util.Properties.envOrNone

object LanguageControlStream {


  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    properties.setProperty(TwitterSource.CONSUMER_KEY, envOrNone("CONSUMER_KEY").get)
    properties.setProperty(TwitterSource.CONSUMER_SECRET, envOrNone("CONSUMER_SECRET").get)
    properties.setProperty(TwitterSource.TOKEN, envOrNone("TOKEN").get)
    properties.setProperty(TwitterSource.TOKEN_SECRET, envOrNone("TOKEN_SECRET").get)


    class JoinigCoFlapMap extends RichCoFlatMapFunction[Tweet, LanguageConfig, (String,String)] {
      val shouldProcess:ValueStateDescriptor[Boolean] = new ValueStateDescriptor[Boolean]("languageConfig",classOf[Boolean])

      override def flatMap1(tweet: Tweet, out: Collector[(String, String)]): Unit = {
        val processLanguage:Boolean = getRuntimeContext().getState(shouldProcess).value()
        Option(processLanguage)
          .map(v =>
            if(v)
              for (tag:String <- tweet.tags)
            out.collect(tweet.language, tag))
      }

      override def flatMap2(languageConfig: LanguageConfig, out: Collector[(String, String)]): Unit = {
        getRuntimeContext().getState(shouldProcess).update(languageConfig.shouldProcess)
      }
    }

    val controlStream:DataStream[LanguageConfig] = env.socketTextStream("localhost", 9876)
      .flatMap((langConfig:String, col:Collector[LanguageConfig]) => {
        for ( s:String <- langConfig.split(",")) {
          val arr:Array[String] = s.split("=")
          col.collect(new LanguageConfig(arr(0), arr(1).toBoolean))
        }
      })

    val mapper = new ObjectMapper()


    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.addSource(new TwitterSource(properties)).map(s => new TwitterMapper().map(s):Tweet)
      .keyBy( tweet => tweet.language).connect(controlStream.keyBy((l:LanguageConfig)=>l.language))
      .flatMap (new JoinigCoFlapMap()).print()

    env.execute("Language Control Stream")
  }
}
