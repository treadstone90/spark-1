import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

trait TitleMatcher {
  def predictTitle(questionRDD: RDD[Post], input: Post): Title

  def evaluateAccuracy(questionRDD:RDD[Post], predictedTitle: RDD[Title]) = {
    val joined = predictedTitle.map(title => (title.postId, title.titleId))
        .join(questionRDD.map(post => (post.id, post.title)))

    val correct = joined.filter{case (key, (pred, gold)) => pred == gold.titleId}.count
    val totalSize = joined.count()

    println("correct is " + correct)
    println("totalSize is " + totalSize)
    1.0*correct/totalSize
  }
}
