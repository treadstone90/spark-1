import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

/**
 * Created by karthik on 10/11/14.
 */
class WordJaccardMatcher(stopWordsFile: String)  extends TitleMatcher with java.io.Serializable {
  lazy val stopWords: Set[String] = scala.io.Source.fromFile(stopWordsFile)
      .getLines.toSet

  def predictTitle(questionRDD: RDD[Post], input: Post): Title = {
    println("hi")
    val tokenizedQuestion = input.body.split(" ").map(_.toLowerCase).toSet

    val predictedTitle = questionRDD.reduce { (prev: Post, current: Post) =>
      if( sortFunction(tokenizedQuestion, current) > sortFunction(tokenizedQuestion, prev)) {
        current
      }
      else {
        prev
      }
    }
    predictedTitle.title
  }

  private def sortFunction(tokenizedQuestion: Set[String], question: Post) = {
    val title = question.title
    val titleWords = title.text.split(" ").map(_.toLowerCase).filterNot(stopWords)
    val similarity = jaccardSimilarity(titleWords.toSet, tokenizedQuestion)
    -similarity
  }

  private def jaccardSimilarity[T](set1: Set[T], set2: Set[T]): Double = {
    1.0*(set1.intersect(set2).size)/ (set1.union(set2).size)
  }

}