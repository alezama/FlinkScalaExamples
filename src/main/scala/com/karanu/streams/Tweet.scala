package com.karanu.streams


class Tweet {

  var language:String = _
  var text:String=_
   var tags:List[String]=_

  override def toString: String = s"Tweet{language: $language, text: $text, tags: $tags}"

}
