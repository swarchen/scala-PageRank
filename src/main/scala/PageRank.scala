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
        val counts = lines.flatMap (line => {
            val titlePattern = "<title>(.+?)</title>".r //define regex of title string out from <title> tag
            val linkPattern = "\\[\\[(.+?)([\\|#]|\\]\\])".r  //define regex of link string out from [[]] area          
            val title = titlePattern.findFirstMatchIn(line).get.group(1) // find title by defined regex

            //find all link in pages and do map reduce
            linkPattern.findAllIn(line).matchData.map(link => {
              val corlink = link.group(1).replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&").replaceAll("&quot;", "\"").replaceAll("&apos;", "\'")
              val cortitle = title.replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&").replaceAll("&quot;", "\"").replaceAll("&apos;", "\'")
              (cortitle -> corlink)
            })               
        }).reduceByKey(_ ++ _)
        
        counts.saveAsTextFile(outputPath) // Output
        sc.stop
    }
}
