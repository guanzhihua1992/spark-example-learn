package org.apache.spark.examples.graphx

import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.sql.SparkSession

object AggregateMessagesExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext
    val graph: Graph[Double, Int] =
      GraphGenerators.logNormalGraph(sc, numVertices = 5).mapVertices((id, _) => id.toDouble)

    println(s"start print graph")
    println(graph.edges.collect().foreach(print(_)))
    println(graph.vertices.collect().foreach(print(_)))
    println(graph.triplets.collect().foreach(print(_)))
    println(s"end print graph")

    val olderFollowers: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)](
      triplet => {
        if (triplet.srcAttr > triplet.dstAttr) {
          triplet.sendToDst((1, triplet.srcAttr))
        }
      },
      (a, b) => (a._1 + b._1, a._2 + b._2)
    )
    println(s"start print olderFollowers")
    println(olderFollowers.collect.foreach(print(_)))
    println(s"end print olderFollowers")
    val avgAgeOfOlderFollowers: VertexRDD[Double] = {
      olderFollowers.mapValues((id, value) =>
        value match {
          case (count, totalAge) => totalAge / count
        })
    }
    println(s"start print avgAgeOfOlderFollowers")
    println(avgAgeOfOlderFollowers.collect.foreach(print(_)))
    println(s"end print avgAgeOfOlderFollowers")
    spark.stop()
  }
}
