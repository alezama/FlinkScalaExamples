package com.karanu

/**
  * Class for storing movie records
  * @param name Name of the movie
  * @param genres The set of genres for the movie
  */

class Movie (val name: String, val genres:Set[String]) {

  override def toString: String = {
    s"Movie{name=$name, genres=$genres}"
  }


}
