import java.util.UUID
import org.apache.spark.rdd.RDD
import org.xml.sax.SAXException

object StackOverFlowDump {
  import PostTypeId._

  def readXMLAsRDD(lines: RDD[String]): RDD[Post] = {
    lines.flatMap(toPost)
  }

  def toPost(line: String): Option[Post] = {
    try {
      val xml = scala.xml.XML.loadString(line)
      val id = clean((xml \ "@Id").text).toLong
      Some(Post(
        id,
        getPostType(clean((xml \ "@PostTypeId").text).toInt),
        clean((xml \ "@AcceptedAnswerId").text).toLong,
        clean((xml \ "@ParentId").text).toLong,
        clean((xml \ "@CreationDate").text),
        clean((xml \ "@Score").text).toInt,
        clean((xml \ "@ViewCount").text).toInt,
        clean((xml \ "@Body").text),
        Title(UUID.randomUUID, id, clean((xml \ "@Title").text)),
        (xml \ "@Tags").text.trim,
        clean((xml \ "@AnswerCount").text).toInt,
        clean((xml \ "@CommentCount").text).toInt,
        clean((xml \ "@FavoriteCount").text).toInt
      ))
    }
    catch {
      case e: SAXException => {
        println("invalid xml")
        None
      }
    }
  }
  
  def readXML(lines: Iterator[String]): Iterator[Post] = {
    lines.flatMap(toPost)
  }

  private def clean(str: String) = {
    if(str == "") {
      "-1"
    }
    else {
      str.replaceAll( """<(?!\/?a(?=>|\s.*>))\/?.*?>""", "")
          .trim
          .split("\n").mkString(" ")
    }
  }

  private def getPostType(id: Int): PostType = {
    id match {
      case 1 => Question
      case 2 => Answer
      case _ => Others
    }
  }

}
