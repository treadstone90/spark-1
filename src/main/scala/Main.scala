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

    val postRDD = StackOverFlowDump.readSOXml(fileRDD)
    val questionRDD: RDD[Post] = postRDD.filter(_.parentId == -1).cache()

    val matcher1 = new WordJaccardMatcher(stopWordsFile)
    val questionSamples = questionRDD.take(1000)

    questionRDD.flatMap(post => List.fill(20)(post))
    val titles = questionSamples.map(matcher1.predictTitle(questionRDD,_))
    println(matcher1.evaluateAccuracy(questionRDD, sc.parallelize(titles)))
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