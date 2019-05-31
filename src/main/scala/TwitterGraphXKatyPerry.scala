import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.Union
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

class TwitterGraphXKatyPerry {

  def run: Unit = {

    /*val sc = SparkSession
      .builder
      .appName("Twitter GraphX Katy Perry's Followers/Followings")
      .master("local")
      .getOrCreate*/

    val conf = new SparkConf().setAppName("KMeansExample").set("spark.driver.allowMultipleContexts", "true").setMaster("local")
    val sc = new SparkContext(conf)

    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    System.in.read

    /*val userIdFollowers = StructField("userIdFollowers", DataTypes.StringType)
    val userIdFollowings = StructField("userIdFollowings", DataTypes.StringType)
    val nameFollowers = StructField("NameFollowers", DataTypes.StringType)
    val nameFollowings = StructField("NameFollowings", DataTypes.StringType)
    val username = StructField("Username", DataTypes.StringType)
    val twitterJoinedDate = StructField("Twitter Joined Date (UTC)", DataTypes.StringType)
    val verifiedOrNonVerified = StructField("Verified or Non-Verified", DataTypes.StringType)
    val bio = StructField("Bio", DataTypes.StringType)
    val location = StructField("Location", DataTypes.StringType)
    val website = StructField("Website", DataTypes.StringType)
    val accountLanguage = StructField("Account Language", DataTypes.StringType)
    val tweetsCount = StructField("Tweets Count", DataTypes.StringType)
    val followingCount = StructField("Following Count", DataTypes.StringType)
    val followerCount = StructField("Followers Count", DataTypes.StringType)
    val listsCount = StructField("Lists Count", DataTypes.StringType)
    val likesCount = StructField("Likes Count", DataTypes.StringType)
    val profileUrl = StructField("Profile URL", DataTypes.StringType)
    val profilePictureUrl = StructField("Profile Picture URL", DataTypes.StringType)
    val protectedOrNotProtected = StructField("Protected or Not Protected", DataTypes.StringType)
    val lastTweetDate = StructField("Last Tweet Date (UTC)", DataTypes.StringType)

    val fieldsFollowers = Array(userIdFollowers, nameFollowers, username, twitterJoinedDate, verifiedOrNonVerified, bio,
      location, website, accountLanguage, tweetsCount, followingCount, followerCount, listsCount,
      likesCount, profileUrl, profilePictureUrl, protectedOrNotProtected, lastTweetDate)

    val schemaFollowers = StructType(fieldsFollowers)

    val fieldsFollowings = Array(userIdFollowings, nameFollowings, username, twitterJoinedDate, verifiedOrNonVerified, bio,
      location, website, accountLanguage, tweetsCount, followingCount, followerCount, listsCount,
      likesCount, profileUrl, profilePictureUrl, protectedOrNotProtected, lastTweetDate)

    val schemaFollowings = StructType(fieldsFollowings)

    val dataFrameFollowers = sc
      .read
      .schema(schemaFollowers)
      .option("header", "true")
      .csv("C:\\Users\\ruben\\Desktop\\followers_katy_perry.csv")

    val dataFrameFollowings = sc
      .read
      .schema(schemaFollowings)
      .option("header", "true")
      .csv("C:\\Users\\ruben\\Desktop\\followings_katy_perry.csv")

    val id = dataFrameFollowers.select("userIdFollowers")
    val nFollowers = dataFrameFollowers.select("nameFollowers")
    val nFollowings = dataFrameFollowings.select("nameFollowings")

    val followers: RDD[(VertexId, (String, String))] = scontext.parallelize(Seq((id, (nFollowers, nameFollowings))))
    val followings: RDD[(VertexId, (String, String))] = scontext.parallelize(Seq((id, (nFollowings, nameFollowings))))

    val defaultUser = ("John Doe", "Missing")

    val graph = Graph(followers, followings, defaultUser)*/





  }

}
