package org.apache.spark.examples

import org.apache.spark.sql.SparkSession

import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.util.Random

object SparkTC {
  val numEdges = 200
  val numVertices = 100
  val rand = new Random(42)

  def generateGraph: Seq[(Int, Int)] = {
    val edges: mutable.Set[(Int, Int)] = mutable.Set.empty
    while (edges.size < numEdges) {
      val from = rand.nextInt(numVertices)
      val to = rand.nextInt(numVertices)
      if (from != to) edges.+=((from, to))
    }
    edges.toSeq
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkTC")
      .getOrCreate()
    val slices = if (args.length > 0) args(0).toInt else 2
    val graph = generateGraph
    println(graph)
    println(s"before print tc original.")
    var tc = spark.sparkContext.parallelize(graph, slices).cache()
    tc.collect.foreach(println(_))
    println(s"after print tc original.")
    val edges = tc.map(x => (x._2, x._1))
    println(edges)

    var oldCount = 0L
    var nextCount = tc.count()
    do{
      oldCount = nextCount
      println(s"before print tc union with oldCount ${oldCount} nextCount ${nextCount}.")
      tc = tc.union(tc.join(edges).map(x=>(x._2._2, x._2._1))).distinct().cache()
      nextCount = tc.count()
      
    }while(nextCount != oldCount)
    println(s"Tc has ${tc.count()} edges.")
    spark.stop()
  }
}
