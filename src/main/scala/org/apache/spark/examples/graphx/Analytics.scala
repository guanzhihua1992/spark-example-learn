package org.apache.spark.examples.graphx

import org.apache.spark.SparkConf
import org.apache.spark.graphx.GraphXUtils

import scala.collection.mutable

object Analytics {
  def main(args: Array[String]): Unit = {
    if (args.length < 2){
      val usage =
        """Usage: Analytics <taskType> <file> --numEPart=<num_edge_partitions>
          |[other options] Supported 'taskType' as follows:
          |pagerank    Compute PageRank
          |cc          Compute the connected components of vertices
          |triangles   Count the number of triangles""".stripMargin
      System.err.println(usage)
      System.exit(1)
    }
    val taskType = args(0)
    val funcName = args(1)
    val optionList = args.drop(2).map{arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _ => throw new IllegalArgumentException(s"Invalid argument: $arg")
      }
    }
    val options = mutable.Map(optionList: _*)

    val conf = new SparkConf()
    GraphXUtils.registerKryoClasses(conf)

    val numEPart = options.remove("numEPart").map(_.toInt).getOrElse{
      println("Set the number of edge partitions using --numEPart.")
      sys.exit(1)
    }
  }
}
