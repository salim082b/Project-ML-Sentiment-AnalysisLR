package dz.cerist.pgs.bigdata.spark.ml;

import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder};
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
import java.io.File
import java.io.PrintWriter
import scala.io.Source

// sbt package
// spark-shell --master local[4] --packages "org.apache.spark:spark-streaming-twitter_2.11:1.6.3","org.elasticsearch:elasticsearch-spark-20_2.11:5.5.2","databricks:spark-corenlp:0.2.0-s_2.10,edu.stanford.nlp:stanford-corenlp:3.9.1","org.apache.spark:spark-mllib_2.11:2.2.0","org.twitter4j:twitter4j-core:4.0.4","org.twitter4j:twitter4j-stream:4.0.4","org.apache.spark:spark-core_2.11:1.5.2"  --jars /home/hadoop/.ivy2/cache/edu.stanford.nlp/stanford-corenlp/jars/stanford-corenlp-3.9.1-models.jar,/home/hadoop/twitter4j-core-4.0.4.jar,/home/hadoop/twitter4j-stream-4.0.4.jar,/home/hadoop/spark-streaming-twitter_2.11-1.6.3.jar,/home/hadoop/02ProjectTSALR/spark-core_2.11-1.5.2.logging.jar,spark-mllib_2.11-2.2.0.jar
//

//spark-submit --class   dz.cerist.pgs.bigdata.spark.ml.Twitter_ML_LR_CV_Analyse_Sentiment  --master local[10] --packages "org.apache.spark:spark-streaming-twitter_2.11:1.6.3","org.elasticsearch:elasticsearch-spark-20_2.11:6.4.2","databricks:spark-corenlp:0.2.0-s_2.10,edu.stanford.nlp:stanford-corenlp:3.9.1","org.twitter4j:twitter4j-core:4.0.4","org.twitter4j:twitter4j-stream:4.0.4","org.apache.spark:spark-core_2.11:1.5.2"  --jars /home/hadoop/.ivy2/cache/edu.stanford.nlp/stanford-corenlp/jars/stanford-corenlp-3.9.1-models.jar,/home/hadoop/twitter4j-core-4.0.4.jar,/home/hadoop/twitter4j-stream-4.0.4.jar,/home/hadoop/spark-streaming-twitter_2.11-1.6.3.jar,/home/hadoop/02ProjectTSALR/spark-core_2.11-1.5.2.logging.jar /home/hadoop/02ProjectTSALR/target/scala-2.11/twitter_sen_ana_ml_lr_2.11-0.1.jar  consumerKey consumerSecret accessToken accessTokenSecret yoursearchTag  dfsadmin -safemode leave dfsadmin -safemode leave
//

object Twitter_ML_LR_CV_Analyse_Sentiment {

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
  val ssc = new StreamingContext(sc, Durations.seconds(100))
  val sparkSession = SparkSession.builder().getOrCreate();
  import sparkSession.implicits._; 
  
  def covert(a:String):String = {
  
  if (a == "true") return "NIGATIVE" else return "POSITIVE";
  
  }

  def main(args: Array[String]): Unit = {
    
    val url = args(0)
    val has = args(1)
    val ind = args(2)
    val kib = args(3)
     
    val load_model1 = new PrintWriter(new File("/home/hadoop/script_project/SA/send/load_model.txt"))
    load_model1.write("false")
    load_model1.close()

    val conn_tweet1 = new PrintWriter(new File("/home/hadoop/script_project/SA/send/conn_tweet.txt"))
    conn_tweet1.write("false")
    conn_tweet1.close()

    val start_pred1 = new PrintWriter(new File("/home/hadoop/script_project/SA/send/start_pred.txt"))
    start_pred1.write("false")
    start_pred1.close()

    val save_es1 = new PrintWriter(new File("/home/hadoop/script_project/SA/send/save_es.txt"))
    save_es1.write("false")
    save_es1.close()
     
    val filters = List( "#"+has)
    val Tweets = TwitterUtils.createStream(ssc, oAuth,filters)
       //  Tweets.saveAsTextFiles("Twitter/Sentiment"+now)

    val conn_tweet = new PrintWriter(new File("/home/hadoop/script_project/SA/send/conn_tweet.txt"))
    conn_tweet.write("true")
    conn_tweet.close()
    
    val HasthagTweets = Tweets.filter(status => status.getLang == "ar")

    //Hasthag 
    val logisticRegressionModel =  CrossValidatorModel.read.load(url);

    val load_model = new PrintWriter(new File("/home/hadoop/script_project/SA/send/load_model.txt"))
    load_model.write("true")
    load_model.close()
    
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

          val stopwords = sc.textFile("hdfs://master:9000/dz/cerist/pgs/bigdata/raw/list.txt").collect()
          val stopWordsRemovedDF = PreProcessorUtils.stopWordRemover(lemmatizedDF, "lemmas", "filtered_lemmas",stopwords)
                                        .where(size(col("filtered_lemmas")) > 1);

          val featurizedDF = ModelUtils.tfidf(stopWordsRemovedDF, "filtered_lemmas", "features");

          val predictions = logisticRegressionModel.transform(featurizedDF);
          var rows = predictions.select("created_at", "hashtags","user_name", "language", "text", "predicted_label");
          rows = rows.withColumn("sentiment", when(rows("predicted_label") === "true", lit("NIGATIVE")).otherwise(lit("POSITIVE")))
                .select("created_at", "sentiment", "hashtags", "user_name", "language", "text" ); 
          rows.write
              .format("org.elasticsearch.spark.sql")
              //.option("es.ingest.pipeline", "fix_date_"+kib)
              .mode("append")
              .save(ind+"/tweet")
      }

    });

    val start_pred = new PrintWriter(new File("/home/hadoop/script_project/SA/send/start_pred.txt"))
    start_pred.write("true")
    start_pred.close()

    ssc.start()

    val save_es = new PrintWriter(new File("/home/hadoop/script_project/SA/send/save_es.txt"))
    save_es.write("true")
    save_es.close()

    ssc.awaitTermination()

  }
}



