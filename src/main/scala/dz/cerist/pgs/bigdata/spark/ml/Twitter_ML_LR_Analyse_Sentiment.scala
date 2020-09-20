package dz.cerist.pgs.bigdata.spark.ml;

import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder};
import java.util.Date
import java.text.SimpleDateFormat
import java.util.HashMap;
import java.io.Serializable;
import java.util.Properties;
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}
import org.elasticsearch.spark._
import org.apache.spark._;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions._;
import org.apache.spark.sql.types._;
import twitter4j.Status
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
case class tweetCS(USER : String,DATE: String, long : String, Lat : String,hathag : String,Lang : String,Sentiment: String)
// sbt package
// spark-shell --master local[4] --packages "org.apache.spark:spark-streaming-twitter_2.11:1.6.3","org.elasticsearch:elasticsearch-spark-20_2.11:5.5.2","databricks:spark-corenlp:0.2.0-s_2.10,edu.stanford.nlp:stanford-corenlp:3.9.1","org.apache.spark:spark-mllib_2.11:2.2.0","org.twitter4j:twitter4j-core:4.0.4","org.twitter4j:twitter4j-stream:4.0.4","org.apache.spark:spark-core_2.11:1.5.2"  --jars /home/hadoop/.ivy2/cache/edu.stanford.nlp/stanford-corenlp/jars/stanford-corenlp-3.9.1-models.jar,/home/hadoop/twitter4j-core-4.0.4.jar,/home/hadoop/twitter4j-stream-4.0.4.jar,/home/hadoop/spark-streaming-twitter_2.11-1.6.3.jar,/home/hadoop/02ProjectTSALR/spark-core_2.11-1.5.2.logging.jar,spark-mllib_2.11-2.2.0.jar
//

//spark-submit --class   dz.cerist.pgs.bigdata.spark.ml.Twitter_ML_LR_Analyse_Sentiment  --master local[10] --packages "org.apache.spark:spark-streaming-twitter_2.11:1.6.3","org.elasticsearch:elasticsearch-spark-20_2.11:6.4.2","databricks:spark-corenlp:0.2.0-s_2.10,edu.stanford.nlp:stanford-corenlp:3.9.1","org.twitter4j:twitter4j-core:4.0.4","org.twitter4j:twitter4j-stream:4.0.4","org.apache.spark:spark-core_2.11:1.5.2"  --jars /home/hadoop/.ivy2/cache/edu.stanford.nlp/stanford-corenlp/jars/stanford-corenlp-3.9.1-models.jar,/home/hadoop/twitter4j-core-4.0.4.jar,/home/hadoop/twitter4j-stream-4.0.4.jar,/home/hadoop/spark-streaming-twitter_2.11-1.6.3.jar,/home/hadoop/02ProjectTSALR/spark-core_2.11-1.5.2.logging.jar /home/hadoop/02ProjectTSALR/target/scala-2.11/twitter_sen_ana_ml_lr_2.11-0.1.jar  consumerKey consumerSecret accessToken accessTokenSecret yoursearchTag  dfsadmin -safemode leave dfsadmin -safemode leave
//

object Twitter_ML_LR_Analyse_Sentiment {

  val now = "%tY%<tm%<td%<tH%<tM%<tS" format new Date
  val simpleDateFormat = new SimpleDateFormat("EE MMM dd HH:mm:ss ZZ yyyy")
  System.setProperty("twitter4j.oauth.consumerKey", "htPgtYQq23V27L21aG8A8bI4v")
  System.setProperty("twitter4j.oauth.consumerSecret", "UgGoCQPH3mybSIyFexRCYRNUZisBMqtByBbg6oCKOGAWqPkuCF")
  System.setProperty("twitter4j.oauth.accessToken", "1058443672168079360-agWgqK4gGmHSI43LCDmGQSbtAq3XlV")
  System.setProperty("twitter4j.oauth.accessTokenSecret", "FliBh3R9tRSV8OXlivvXz5KXAqoXPSAbKgB4JaA4r9umE")

  val configurationBuilder = new ConfigurationBuilder()
  val oAuth = Some(new OAuthAuthorization(configurationBuilder.build()))

  val conf = new SparkConf()
    .setMaster("local[10]")
    .setAppName("my_app")
   // .set("spark.testing.memory", "2147480000")
   // .set("es.nodes","10.0.243.1")
   // .set("spark.es.port","9200")
   // .set("spark.es.nodes.discovery","true")
   // .set("es.nodes.wan.only","false")
   // .set("spark.es.net.http.auth.pass","")

  val sc = new SparkContext(conf);
  conf.set("es.index.auto.create", "true")
  val ssc = new StreamingContext(sc, Durations.seconds(20))
  val sparkSession = SparkSession.builder().getOrCreate();
  import sparkSession.implicits._; 
  
  def covert(a:String):String = {
  
  if (a == "true") return "NIGATIVE" else return "POSITIVE";
  
  }

  def main(args: Array[String]): Unit = {
    
     val filters = List( "#covid19,conona")
     val Tweets = TwitterUtils.createStream(ssc, oAuth,filters)
       //  Tweets.saveAsTextFiles("Twitter/Sentiment"+now)

    println("hello##################################################4444444444444444")

    val HasthagTweets = Tweets.filter(status => status.getLang == "en")

    //Hasthag 
    val logisticRegressionModel = PipelineModel.read.load("hdfs://master:9000/dz/cerist/pgs/bigdata/spark/logisticRegressionCovid19CVaralg");


    HasthagTweets
      .map(tweet => {
         Row(tweet.getCreatedAt.getTime.toString, tweet.getHashtagEntities.map(_.getText).mkString(" "), tweet.getUser.getScreenName, tweet.getLang.toString(), tweet.getText); 
      })
      .foreachRDD(rdd => {

        if (rdd != null && !rdd.isEmpty()) {

          val tweetsDF = sparkSession.createDataFrame(rdd,
                new StructType().add("created_at", StringType)
                  .add("hashtags", StringType)
                  .add("user_name", StringType)
                  .add("language", StringType)
                  .add("text", StringType));

          val lowercasedDF = PreProcessorUtils.lowercaseRemovePunctuation(tweetsDF, "text");
          
          val lemmatizedDF = lowercasedDF.select("created_at","hashtags","user_name", "language", "text").rdd.mapPartitions(p => {
                p.map{
                    case Row(created_at:String, hashtags:String, user_name:String, language:String, text: String) => (created_at, hashtags, user_name, language, text, text.split(" ").toSeq.map(_.trim).filter(_ != ""));
                };
            }).toDF("created_at", "hashtags","user_name", "language", "text", "lemmas");

          val stopwords = sc.textFile("hdfs://master:9000/dz/cerist/pgs/bigdata/raw/arabic-stop-words-list.txt").collect()
          val stopWordsRemovedDF = PreProcessorUtils.stopWordRemover(lemmatizedDF, "lemmas", "filtered_lemmas",stopwords)
                                        .where(size(col("filtered_lemmas")) > 1);

          val featurizedDF = ModelUtils.tfidf(stopWordsRemovedDF, "filtered_lemmas", "features");

          val predictions = logisticRegressionModel.transform(featurizedDF);
          var rows = predictions.select("created_at", "hashtags","user_name", "language", "text", "predicted_label");
          rows = rows.withColumn("sentiment", when(rows("predicted_label") === "true", lit("NIGATIVE")).otherwise(lit("POSITIVE")))
                .select("created_at", "sentiment", "hashtags", "user_name", "language", "text" ); 
          rows.write
              .format("org.elasticsearch.spark.sql")
              .mode("append")
              .save("covid19_16_05_20/tweet")
      }

    });
    println("hello##################################################555555555555555555")


    ssc.start()

    println("hello##################################################666666666666666666")

    ssc.awaitTermination()

  }
}



