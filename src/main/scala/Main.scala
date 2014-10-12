import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.log4j.{Level, Logger}

/**
 * Created by karthik on 10/5/14.
 */

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("appName").setMaster(args(0))
    val sc = new SparkContext(conf)
    val soDumpFile = args(1)
    val stopWordsFile = args(2)
    val fileRDD = sc.textFile(soDumpFile)
    sc.addFile(stopWordsFile)

    val postRDD = StackOverFlowDump.readXMLAsRDD(fileRDD)
    val questionRDD: RDD[Post] = postRDD.filter(_.parentId == -1).cache()

    val matcher1 = new WordJaccardMatcher(stopWordsFile)
    val questionSamples = questionRDD.take(1000)

    Utils.time("Parallel") {
      val titles = questionSamples.map(matcher1.predictTitle(questionRDD, _))
    }

    //println(matcher1.evaluateAccuracy(questionRDD, sc.parallelize(titles)))
    sc.stop()
  }

  private def countTokenByYear(filteredBodyRDD: RDD[Post], token: String): Map[String, Int] = {
    filteredBodyRDD.filter(_.title.text.toLowerCase.contains(token))
        .map(post => post.creationDate.substring(0, 4) -> 1)
        .groupByKey()
        .mapValues(_.sum)
        .collect()
        .toMap
  }
}


object MainSerial {
  def main(args: Array[String]): Unit = {
    val soDumpFile = args(1)
    val stopWordsFile = args(2)

    val lines = scala.io.Source.fromFile(soDumpFile).getLines()

    val posts = StackOverFlowDump.readXML(lines).toIndexedSeq
    val matcher1 = new WordJaccardMatcher(stopWordsFile)
    val questionSamples = posts.take(1000)
    val titles = questionSamples.map(matcher1.predictTitle(posts, _))
    //println(matcher1.evaluateAccuracy(questionRDD, sc.parallelize(titles)))
  }
}

object MainParallel {
  def main(args: Array[String]): Unit = {
    val soDumpFile = args(1)
    val stopWordsFile = args(2)

    val lines = scala.io.Source.fromFile(soDumpFile).getLines()

    val posts = StackOverFlowDump.readXML(lines).toIndexedSeq.par
    val matcher1 = new WordJaccardMatcher(stopWordsFile)
    val questionSamples = posts.take(1000)
    Utils.time("par seq") {
      val titles = questionSamples.map(matcher1.predictTitle(posts, _))
    }
    //println(matcher1.evaluateAccuracy(questionRDD, sc.parallelize(titles)))
  }
}

object Utils {
  def time[R](str: String)(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: for " + str + " is " + (1.0*(t1 - t0))*(Math.pow(10, -9)) + "sec")
    result
  }
}