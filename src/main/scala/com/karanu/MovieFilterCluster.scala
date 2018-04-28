package com.karanu

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._

/**
 * Skeleton for a Flink Job.
 *
 * For a full example of a Flink Job, see the WordCountJob.scala file in the
 * same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster. Just type
 * {{{
 *   sbt clean assembly
 * }}}
 * in the projects root directory. You will find the jar in
 * target/scala-2.11/Flink\ Project-assembly-0.1-SNAPSHOT.jar
 *
 */

/**
  * Generated using: sbt new tillrohrmann/flink-project.g8
  * Dataset taken from https://grouplens.org/datasets/movielens
  * Flink Program to filer movie records by "Drama" genre
  *
  *
  */
/**
  * Modification of the MovieFilter passing params as execution arguments
  */
object MovieFilterCluster {
  def main(args: Array[String]) {
    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    //parameters
    val parameters = ParameterTool.fromArgs(args)

    // input and output directory
    val input = parameters.getRequired("input")
    val output = parameters.getRequired("output")

    // read the file as a CSV file, columns are of the type Long, String and String
    val csvInput:DataSet[(Long, String, String)] = env.readCsvFile[(Long, String, String)](input,ignoreFirstLine = true, quoteCharacter = '"')

    // The genre column is broken into a set and stored in Movie objects
    val movies:DataSet[Movie] = csvInput.map(line => {
      new Movie(line._2, line._3.split("\\|").toSet)
    }).filter(movie => {
      movie.genres.contains("Drama")
    })

    // records stored as text
    movies.writeAsText(output)

    // execute program
    env.execute("Flink Scala API Skeleton")


  }
}
