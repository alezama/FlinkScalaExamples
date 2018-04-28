package com.karanu.streams

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import scala.collection.JavaConversions.asScalaIterator


import scala.collection.mutable.ListBuffer

class TwitterMapper extends RichMapFunction[String, Tweet] {
  val mapper = new ObjectMapper()

  override def map(s: String): Tweet = {
    val tweetJson = mapper.readTree(s)
    val textNode = Option(tweetJson.get("text"))
    val langNode = Option(tweetJson.get("lang"))
    val entities = Option(tweetJson.get("entities"))


    val tweet = new Tweet()
    tweet.text = textNode match {
      case Some(textNode) => textNode.textValue()
      case None => ""
    }
    tweet.language = langNode match {
      case Some(langNode) => langNode.textValue()
      case None => ""
    }

    var tagBufList = new ListBuffer[String]()

    entities match {
      case Some(entities) =>
        val hashTags = Option(entities.get("hashtags"))
        hashTags match {
          case Some(hashTags) =>
            hashTags.elements().foreach {
              m: JsonNode => tagBufList += m.get("text").textValue()
            }

          case None => ()
        }

      case None => ()
    }
    tweet.tags = tagBufList.toList
    tweet
  }
}