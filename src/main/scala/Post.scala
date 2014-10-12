/**
 * Created by karthik on 10/12/14.
 */
object PostTypeId extends Enumeration {
  type PostType= Value
  val Question, Answer, Others = Value
}

import java.util.UUID

import PostTypeId._
case class Post(id: Long,
                postTypeId: PostType,
                acceptedAnswerId: Long,
                parentId: Long,
                creationDate: String,
                score: Int,
                viewCount: Int,
                body: String,
                title: Title,
                tags: String,
                answerCount: Int,
                commentCount: Int,
                favoriteCount: Int
                   )


case class TokenizedPosts(id: Long,
                          text: String,
                          postTypeId: PostType,
                          body: Seq[String],
                          title: String
                             )

case class Title(titleId: UUID, postId: Long, text: String)