package com.karanu

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector


/**
  * Reads the input file of movies and ratings and obtain the ten top best rated movies, the condition is that the movie
  * has at least 50 reviews. The algorithm divides the initial data into 5 partition, each partition is descending
  * ordered to get the local top 10, then the top 10 results of echa partition are collected in a single partition and
  * sorted for the final result.
  */
object Top10Movies {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    // Input rating file path CSV file as (userId,movieId,rating,timestamp)
    val inputRatings = "src/main/resources/ml-latest-small/ratings.csv"

    // The partition implementation uses the rating to divide the data, assuming that the rating is in range of [5,0]
    val partitionerByRating: Partitioner[Double] = new Partitioner[Double]() {
      override def partition(key: Double, numPartitions: Int): Int = key.intValue() - 1
    }

    // Input movie file path CSV file as (movieId,title,genres)
    val inputMovies = "src/main/resources/ml-latest-small/movies.csv"


    // Read the file only taking columns 2 and 3 (movieId,rating)
    val sorted: DataSet[(Long, Double)] = env.readCsvFile[(Long, Double)](inputRatings,
      includedFields = Array(1, 2), ignoreFirstLine = true)
      // Group by movieId
      .groupBy(0)
      // Reduce the group by only taking movies with more than 50 reviews, the reduce group contains the movieId and
      // the average rating
      .reduceGroup[(Long, Double)] { (it: Iterator[(Long, Double)], col: Collector[(Long, Double)]) => {
      var movieId = 0L
      var total = 0.0
      var count = 0
      for ((movieIdIt, rating) <- it) {
        movieId = movieIdIt
        count += 1
        total += rating
      }

      if (count > 50) {
        col.collect((movieId, total / count))
      }
    }
    }

      // Divides the dataset using the rating, therefore ratings of 5 gets into the 5th partition
      // ratings of (5-4] get into the 4th partition and so on.
      .partitionCustom(partitionerByRating, 1)
      // Use 5 process one per collection
      .setParallelism(5)
      // Sort the data locally
      .sortPartition(1, Order.DESCENDING)
      // Get the 10 top results per partion
      .mapPartition((it: Iterator[(Long, Double)], col: Collector[(Long, Double)]) => {
        for (i <- 1 to 10 if it.hasNext) col.collect(it.next())
      })
      // Collect the data in one process and sort it
      .sortPartition(1, Order.DESCENDING)
      .setParallelism(1)
      // Get the global 10 top results
      .mapPartition((it: Iterator[(Long, Double)], col: Collector[(Long, Double)]) => {
        for (i <- 1 to 10 if it.hasNext) col.collect(it.next())
      })

    // Read the movie file  taking all columns  (movieId,title,genres)
    val movies: DataSet[(Long, String, String)] = env.readCsvFile[(Long,
      String, String)](inputMovies, ignoreFirstLine = true, quoteCharacter = '"')

    // Join the movies and rating data set by the movieId column and creates a new data set with movieId,
    // movieTitle and rating
    movies.join(sorted)
      .where(0)
      .equalTo(0) { (movie, rating) => (movie._1, movie._2, rating._2) }.print()

  }
}