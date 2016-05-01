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
            val titlePattern = "<title>(.+?)</title>".r
            val linkPattern = "\\[\\[(.+?)([\\|#]|\\]\\])".r            
            val title = titlePattern.findFirstMatchIn(line).get.group(1)
            linkPattern.findAllIn(line).matchData.map(link => {
              val corlink = link.group(1).replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&").replaceAll("&quot;", "\"").replaceAll("&apos;", "\'")
              val cortitle = title.replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&").replaceAll("&quot;", "\"").replaceAll("&apos;", "\'")
              (cortitle -> corlink)
            })               
        }).reduceByKey(_ +","+ _) 
        counts.saveAsTextFile(outputPath) // Output
        sc.stop
    }
}
