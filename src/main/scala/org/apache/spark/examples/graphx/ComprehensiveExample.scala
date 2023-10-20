package org.apache.spark.examples.graphx

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession

object ComprehensiveExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext
    val users = (sc.textFile("data/graphx/users.txt")
      .map(line => line.split(",")).map(parts => (parts.head.toLong, parts.tail)))

    val followerGraph = GraphLoader.edgeListFile(sc, "data/graphx/followers.txt")

    val graph = followerGraph.outerJoinVertices(users) {
      case (uid, deg, Some(attrList)) => attrList
      case (uid, deg, None) => Array.empty[String]
    }
    val subgraph = graph.subgraph(vpred = (vid, attr) => attr.size == 2)

    val pagerankGraph = subgraph.pageRank(0.001)

    val userInfoWithPageRank = subgraph.outerJoinVertices(pagerankGraph.vertices){
      case (uid, attrList, Some(pr)) => (pr, attrList.toList)
      case (uid, attrList, None) => (0.0, attrList.toList)
    }
    println(userInfoWithPageRank.vertices.top(5)(Ordering.by(_._2._1)).mkString("\n"))
    // $example off$

    spark.stop()
  }
}
