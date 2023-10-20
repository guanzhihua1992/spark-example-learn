
// scalastyle:off println
package org.apache.spark.examples.graphx

/**
 * Uses GraphX to run PageRank on a LiveJournal social network graph. Download the dataset from
 * http://snap.stanford.edu/data/soc-LiveJournal1.html.
 */
object LiveJournalPageRank {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        "Usage: LiveJournalPageRank <edge_list_file>\n" +
          "    --numEPart=<num_edge_partitions>\n" +
          "        The number of partitions for the graph's edge RDD.\n" +
          "    [--tol=<tolerance>]\n" +
          "        The tolerance allowed at convergence (smaller => more accurate). Default is " +
          "0.001.\n" +
          "    [--output=<output_file>]\n" +
          "        If specified, the file to write the ranks to.\n" +
          "    [--partStrategy=RandomVertexCut | EdgePartition1D | EdgePartition2D | " +
          "CanonicalRandomVertexCut]\n" +
          "        The way edges are assigned to edge partitions. Default is RandomVertexCut.")
      System.exit(-1)
    }

    Analytics.main(args.patch(0, List("pagerank"), 0))
  }
}
// scalastyle:on println
