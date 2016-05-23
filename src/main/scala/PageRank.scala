/* PageRank.scala */
import org.apache.spark._
import org.apache.hadoop.fs._

import scala.collection.mutable.ListBuffer

object PageRank {
    def main(args: Array[String]) {
        val files = "/shared/HW2/sample-in/input-1G"
        val outputPath = "hw2/pagerank-1G/result"
        val conf = new SparkConf().setAppName("PageRank")
        val sc = new SparkContext(conf)

        // Cleanup output dir
        val hadoopConf = sc.hadoopConfiguration
        val hdfs = FileSystem.get(hadoopConf)
        try { hdfs.delete(new Path(outputPath), true) } catch { case _ : Throwable => { } }
        val lines = sc.textFile(files)
        val graph = lines.flatMap (line => {
            val kvPair = new ListBuffer[(String, String)]()
            val titlePattern = "<title>(.+?)</title>".r //define regex of title string out from <title> tag
            val linkPattern = "\\[\\[(.+?)([\\|#]|\\]\\])".r  //define regex of link string out from [[]] area          
            val title = titlePattern.findFirstMatchIn(line).get.group(1) // find title by defined regex
            var cortitle = title.replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&").replaceAll("&quot;", "\"").replaceAll("&apos;", "\'")
              cortitle = Character.toUpperCase(cortitle.charAt(0)) + cortitle.substring(1)
            kvPair.append((cortitle, "@"))
            //find all link in pages and do map job

            linkPattern.findAllIn(line).matchData foreach(link => {
              var corlink = link.group(1).replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&").replaceAll("&quot;", "\"").replaceAll("&apos;", "\'")
                kvPair.append((cortitle, ">\t" + corlink))
                kvPair.append((corlink, "<\t" + cortitle))

            })
            kvPair.toList
        }).groupByKey().flatMap(data => {
        var valid = false
        val list = new ListBuffer[(String, String)]
        val srcList = new ListBuffer[String]
        val dstList = new ListBuffer[String]
        for (links <- data._2) {
          val linkValues: Array[String] = links.split("\t")
          val direction: String = linkValues(0)
          if (direction == "@") {
            valid = true
          } else {
            val v: String = linkValues(1)
            if (direction == ">") {
              dstList.append(v)
            } else if (direction == "<") {
              srcList.append(v)
            }
          }
        }

        if (valid) {
          // to-link
          val dstSB: StringBuilder = new StringBuilder
          dstSB.append(">\t")
          for (dstLink <- dstList) {
            dstSB.append(dstLink).append("\t")
          }
          list.append((data._1, dstSB.toString))

          // from-link
          val srcSB: StringBuilder = new StringBuilder
          srcSB.append("<\t")
          for (srcLink <- srcList) {
            srcSB.append(srcLink).append("\t")
          }
          list.append((data._1, srcSB.toString))
        }
        list
        })
        
        val fullGraph = graph.flatMap(row => {
            val lb = new ListBuffer[(String, String)]
            val links = row._2.split("\t")
            val direction = links(0)
            val title = row._1
            if (direction == ">"){
                lb.append((title, "@"))
            }else if (direction == "<"){
                for (idx <- 1 until links.length){
                    lb.append((links(idx), title))
                }
            }else{
                lb.append((title, "error"))
            }
            lb
            }).groupByKey()
        
        var fg = fullGraph.mapValues(info => {
          var isValid = false
          for (str <- info) {
            if (str == "@") {
              isValid = true
            }
          }
          if (isValid)
            info.filter(_ != "@").toSeq.mkString("\t") + "\t"
          else
            ""
        })
        fg = fg.filter(_._2 != "")
        //fg.sortByKey().saveAsTextFile("hw2/pagerank/fg")

        val links = fg.map(line => {
            val words = line._2.split("\t")
            val src = line._1
            val outDegree = words.length
            if (outDegree == 0) {
            (src, List("00")) 
            } else {
            (src,
              for (di <- (0 until words.length).toList) yield {
                words(di)
              })
            }
        }).cache()
        //val test = graph.map{ case( pre, cur ) => {(cur._1 -> cur._2)}}.groupByKey
        val totalNum = links.count()
        val alpha = 0.85
        var ranks = links.mapValues (v => (1.0/totalNum))
        var oldPR = 0.0
        var newPR = 1.0
        var ranks_diff_list = new ListBuffer[Double]()
        while ((newPR - oldPR).abs > 0.001){
            oldPR = ranks.map(_._2).sum
            val contributes = links.join(ranks).values.flatMap { case (urls, rank) =>
              val size = urls.size
              urls.map(url => (url, rank / size))
            }
            val rankZeroList = contributes.lookup("00")
            var rankZero = 0.0
            if (rankZeroList.nonEmpty) {
              rankZero = rankZeroList.sum
            }

            val ranksBase = links.mapValues(_ => (1 - alpha) / totalNum + alpha * rankZero / totalNum)
            val ranksIncome = contributes.reduceByKey(_ + _).mapValues(alpha * _)

            val newRanks = ranksBase.union(ranksIncome).reduceByKey(_ + _)
            ranks = newRanks
            newPR = newRanks.map(_._2).sum
            ranks_diff_list += (newPR - oldPR).abs
        }
        var result = ranks.sortBy{ case(url,pr) => (- pr , url) }
        
        result = result.filter(_._1 != "00")
        result.map(x => x._1 + "\t" + x._2).saveAsTextFile(outputPath) // Output
        ranks_diff_list.foreach(println)
        sc.stop
        //Wikipedia:Deletion review
    }
}
