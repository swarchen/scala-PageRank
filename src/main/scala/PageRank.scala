/* PageRank.scala */
import org.apache.spark._
import org.apache.hadoop.fs._

object PageRank {
    def main(args: Array[String]) {
        val files = "/shared/HW2/sample-in/input-100M"
        val outputPath = "hw2/pagerank"
        val conf = new SparkConf().setAppName("PageRank")
        val sc = new SparkContext(conf)

        // Cleanup output dir
        val hadoopConf = sc.hadoopConfiguration
        val hdfs = FileSystem.get(hadoopConf)
        try { hdfs.delete(new Path(outputPath), true) } catch { case _ : Throwable => { } }
        val lines = sc.textFile(files)
        val graph = lines.flatMap (line => {
            val titlePattern = "<title>(.+?)</title>".r //define regex of title string out from <title> tag
            val linkPattern = "\\[\\[(.+?)([\\|#]|\\]\\])".r  //define regex of link string out from [[]] area          
            val title = titlePattern.findFirstMatchIn(line).get.group(1) // find title by defined regex

            //find all link in pages and do map job
            linkPattern.findAllIn(line).matchData.map(link => {
              val corlink = link.group(1).replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&").replaceAll("&quot;", "\"").replaceAll("&apos;", "\'")
              val cortitle = title.replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&").replaceAll("&quot;", "\"").replaceAll("&apos;", "\'")
                (cortitle -> corlink)
            })
        }).groupByKey
        
        //val test = graph.map{ case( pre, cur ) => {(cur._1 -> cur._2)}}.groupByKey
        val totalNum = graph.count()
        var ranks = graph.mapValues (v => (1/totalNum).toDouble)
        var oldPR = 0.0
        var newPR = 1.0
        while ((newPR - oldPR).abs > 0.001){
        //for(i <- 1 to 22){
            oldPR = ranks.map(_._2).sum
            val contributions = graph.join(ranks).flatMap{
                case(url, (links, rank)) => links.map(dest => (dest, rank / links.size))
            }
            ranks = contributions.reduceByKey((x, y) => x+ y).mapValues(v => 0.15 / totalNum + 0.85*v)
            newPR = ranks.map(_._2).sum
        }
        val result = ranks.sortBy{ case(url,pr) => (- pr , url) }
        result.saveAsTextFile(outputPath) // Output
        sc.stop
        //Wikipedia:Deletion review
    }
}
