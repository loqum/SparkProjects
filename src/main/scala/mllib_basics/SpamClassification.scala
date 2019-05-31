package mllib_basics

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionWithSGD}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

class SpamClassification {

  def spamClassification: Unit = {
    /*val spark = SparkSession
      .builder
      .appName("Spam Classification")
      .master("local")
      .getOrCreate()*/

    val conf = new SparkConf().setAppName("KMeansExample").set("spark.driver.allowMultipleContexts", "true").setMaster("local")
    val sc = new SparkContext(conf)

    val spam = sc.textFile("spam_samples\\spam.txt")
    val normal = sc.textFile("spam_samples\\normal.txt")

    // Create a HashingTF instance to map email text to vectors of 10,000 features.
    val tf = new HashingTF(numFeatures = 10000)

    // Each email is split into words, and each word is mapped to one feature.
    val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
    val normalFeatures = normal.map(email => tf.transform(email.split(" ")))

    // Create LabeledPoint datasets for positive (spam) and negative (normal) examples.
    val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))
    val negativeExamples = normalFeatures.map(features => LabeledPoint(0, features))
    val trainingData = positiveExamples.union(negativeExamples)

    trainingData.cache() // Cache since Logistic Regression is an iterative algorithm.

    // Run Logistic Regression using the LBFGS algorithm.
    val model = new LogisticRegressionWithLBFGS().run(trainingData)

    // Test on a positive example (spam) and a negative one (normal).
    val posTest = tf.transform(
      "Dear sir, I am a Prince in a far kingdom you have not heard of.  I want to send you money via wire transfer so please ...".split(" "))
    val negTest = tf.transform(
      "Can you hack it? You can now with the Humble Book Bundle: Hacking 2.0 by No Starch Press ...".split(" "))

    val predictPosTest = model.predict(posTest)
    val predictNegTest = model.predict(negTest)

    println("Prediction for positive test example: " + predictPosTest)
    println("Prediction for negative test example: " + predictNegTest)

    /*val url = "jdbc:mysql://localhost:3306/sample?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val dbtable = "spam_classification"
    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")

    df.write.mode("append").jdbc(url, dbtable, prop)*/
  }

}
