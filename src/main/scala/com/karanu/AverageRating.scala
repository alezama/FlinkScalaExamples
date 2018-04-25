
package com.karanu


import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

/**
  * Combines two data sets and performs the Average Rating over the first genre of the data set
  */

object AverageRating {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val inputMovies = "src/main/resources/ml-latest-small/movies.csv"
    val inputRatings = "src/main/resources/ml-latest-small/ratings.csv"

    //Read the data set as a CSV File

    val movies:DataSet[(Long,String,String)] = env.readCsvFile[(Long, String, String)](inputMovies,ignoreFirstLine = true,quoteCharacter = '"')

    val rating:DataSet[(Long,Double)] = env.readCsvFile[(Long,Double)](inputRatings,ignoreFirstLine = true,includedFields = Array(1,2))

    // Combine both datasets where the id of the movie is equal, then creates a new data set with the second column of movies (movie namme, the first genre of the movie
    // after splitting and the movie rating from the rating dataset, the dataset is group by the second field genre
    val moviesGenre:GroupedDataSet[(String,String,Double)]=movies.join(rating).where(0).equalTo(0) {(l, r) => (l._2, l._3.split("\\|")(0),r._2)}
      .groupBy(1)

    // the group is reduced calculating the average rating by genre, all the recods of the same genre are avergae by rating
    val moviesList = moviesGenre.reduceGroup {(it:Iterator[(String, String, Double)], out: Collector[(String, Double)]) => {
      var sum = 0
      var totalScore = 0.0
      var outGenre: String = null

      for ((name: String, genre: String, count: Double) <- it) {
        totalScore += count
        sum += 1
        outGenre = genre
      }
      out.collect(outGenre, totalScore / sum)

    }
    }.collect()

    // the results are collected and sorted locally
    // Output:
    // ArrayBuffer((Sci-Fi,3.1594202898550723),
    // (Horror,3.1685996563573884), (Romance,3.2333333333333334), (Children,3.300751879699248),
    // (Action,3.4456128030751034), (Comedy,3.471038550169902), (Thriller,3.49438202247191),
    // (Fantasy,3.5165016501650164), (Adventure,3.5751261352169528), (Animation,3.601991150442478),
    // (Western,3.6115702479338845), (Musical,3.6439024390243904), (Drama,3.675283412129531),
    // ((no genres listed),3.7777777777777777), (Documentary,3.835063009636768), (Crime,3.858596134282808),
    // (War,3.8846153846153846), (Mystery,3.9024134312696748), (Film-Noir,4.054216867469879))
    println(moviesList.sortWith(_._2 < _._2))



  }

}