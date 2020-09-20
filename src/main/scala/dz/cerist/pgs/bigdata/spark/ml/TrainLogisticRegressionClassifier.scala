package dz.cerist.pgs.bigdata.spark.ml; 

import edu.stanford.nlp.pipeline.StanfordCoreNLP;

import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.functions._;
import org.apache.spark.sql.types._;
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder};
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import java.io.File
import java.io.PrintWriter

import scala.io.Source
import scala.collection.JavaConversions._;

/**
 * Tweet Sentiment Decision Tree Classifier
 * Train a Decision Tree Classifier on a collection of pre-labelled tweets about airlines
 *
 * @author jillur.quddus
 * @version 0.0.1

spark-shell --packages "org.apache.spark:spark-streaming-twitter_2.11:1.6.3","org.elasticsearch:elasticsearch-spark-20_2.10:6.7.0",databricks:spark-corenlp:0.2.0-s_2.10,edu.stanford.nlp:stanford-corenlp:3.9.1,"org.apache.spark:spark-mllib_2.11:2.2.0"  --jars stanford-corenlp-3.9.1-models.jar,spark-mllib_2.11-2.2.0.jar --driver-library-path /home/hadoop/.ivy2/cache/edu.stanford.nlp/stanford-corenlp/jars/stanford-corenlp-3.9.1-models.jar:/home/hadoop/.cache/coursier/v1/https/repo1.maven.org/maven2/org/apache/spark/spark-mllib_2.11/2.2.0/spark-mllib_2.11-2.2.0.jar

spark-submit --class dz.cerist.pgs.bigdata.spark.ml.TrainLogisticRegressionClassifier --master local[4] --driver-memory 3g --executor-memory 3g --packages "org.apache.spark:spark-streaming-twitter_2.11:1.6.3","org.elasticsearch:elasticsearch-spark-20_2.10:6.7.0",databricks:spark-corenlp:0.2.0-s_2.10,edu.stanford.nlp:stanford-corenlp:3.9.1,"org.apache.spark:spark-mllib_2.11:2.2.0"  --jars stanford-corenlp-3.9.1-models.jar,spark-mllib_2.11-2.2.0.jar --driver-library-path /home/hadoop/.ivy2/cache/edu.stanford.nlp/stanford-corenlp/jars/stanford-corenlp-3.9.1-models.jar:/home/hadoop/.cache/coursier/v1/https/repo1.maven.org/maven2/org/apache/spark/spark-mllib_2.11/2.2.0/spark-mllib_2.11-2.2.0.jar  /home/hadoop/02ProjectTSALR/target/scala-2.11/twitter_sen_ana_ml_lr_2.11-0.1.jar  consumerKey consumerSecret accessToken accessTokenSecret yoursearchTag  dfsadmin -safemode leave dfsadmin -safemode leave

 */

object TrainLogisticRegressionClassifier {

  def main(args: Array[String]) = {

    /********************************************************************
     * SPARK CONTEXT
     ********************************************************************/

    // Create the Spark Context
    val url = args(0)
    val mod = args(1)
    
    val conf = new SparkConf()
      .setAppName("Sentiment Models")
      .setMaster("local[4]");

    val sc = new SparkContext(conf);
    val sparkSession = SparkSession.builder().getOrCreate();
    import sparkSession.implicits._;

    val load_ds1 = new PrintWriter(new File("/home/hadoop/script_project/ML/send/load_dataset.txt"))
    load_ds1.write("false")
    load_ds1.close()

    val remove_nalph1 = new PrintWriter(new File("/home/hadoop/script_project/ML/send/remove_nalph.txt"))
    remove_nalph1.write("false")
    remove_nalph1.close()

    val lemm_word1 = new PrintWriter(new File("/home/hadoop/script_project/ML/send/lemm_word.txt"))
    lemm_word1.write("false")
    lemm_word1.close()

    val stop_word1 = new PrintWriter(new File("/home/hadoop/script_project/ML/send/stop_word.txt"))
    stop_word1.write("false")
    stop_word1.close()

    val featurize1 = new PrintWriter(new File("/home/hadoop/script_project/ML/send/featurize.txt"))
    featurize1.write("false")
    featurize1.close()

    val arraycv1 = new PrintWriter(new File("/home/hadoop/script_project/ML/send/arraycv.txt"))
    arraycv1.write("")
    arraycv1.close()

    val bestcv1 = new PrintWriter(new File("/home/hadoop/script_project/ML/send/bestcv.txt"))
    bestcv1.write("")
    bestcv1.close()

    val paramcv1 = new PrintWriter(new File("/home/hadoop/script_project/ML/send/paramcv.txt"))
    paramcv1.write("")
    paramcv1.close()

    val accuracy1 = new PrintWriter(new File("/home/hadoop/script_project/ML/send/accuracy.txt"))
    accuracy1.write("")
    accuracy1.close()

    val metric1 = new PrintWriter(new File("/home/hadoop/script_project/ML/send/metric.txt"))
    metric1.write("")
    metric1.close()

    val savemodel1 = new PrintWriter(new File("/home/hadoop/script_project/ML/send/savemodel.txt"))
    savemodel1.write("false")
    savemodel1.close()

    val fin1 = new PrintWriter(new File("/home/hadoop/script_project/ML/send/fin.txt"))
    fin1.write("false")
    fin1.close()

    /********************************************************************
     * INGEST THE CORPUS
     ********************************************************************/

    // Define the CSV Dataset Schema
    val schema = new StructType(Array(
                    StructField("id", StringType, true),
                    StructField("text", StringType, true),
                    StructField("sentiment", StringType, true)
                  ));

    // Read the CSV Dataset, keeping only those columns that we need to build our model
    var tweetsDF = SparkSession.builder().getOrCreate().read
                      .format("csv")
                      .option("header", true)
                      .option("delimiter", ",")
                      .option("mode", "DROPMALFORMED")
                      .schema(schema)
                      //.load("hdfs://master:9000/dz/cerist/pgs/bigdata/raw/dataset-arabic-algerien.csv")
                      .load(url)
                      .select("sentiment", "text");

    /********************************************************************
     * LABEL THE DATA
     ********************************************************************/

    // We are interested in detecting tweets with negative sentiment. Let us create a new column whereby
    // if the sentiment is negative, this new column is TRUE (Positive Outcome), and FALSE (Negative Outcome)
    // in all other cases

    tweetsDF = tweetsDF.withColumn("negative_sentiment_label", when(tweetsDF("sentiment") === "Negative", lit("true")).otherwise(lit("false")))
                .select("text", "negative_sentiment_label");

    val load_ds = new PrintWriter(new File("/home/hadoop/script_project/ML/send/load_dataset.txt"))
    load_ds.write("true")
    load_ds.close()



    /********************************************************************
     * APPLY THE PRE-PROCESSING PIPELINE
     ********************************************************************/

    // Let us now perform some simple pre-processing including converting the text column to lowercase
    // and removing all non-alphanumeric characters

    val lowercasedDF = PreProcessorUtils.lowercaseRemovePunctuation(tweetsDF, "text");

    val remove_nalph = new PrintWriter(new File("/home/hadoop/script_project/ML/send/remove_nalph.txt"))
    remove_nalph.write("true")
    remove_nalph.close()

    // Lemmatize the text to generate a sequence of Lemmas using the Stanford NLP Library
    // By using mapPartitions, we create the Stanford NLP Pipeline once per partition rather than once per RDD entry

    val lemmatizedDF = lowercasedDF.select("text", "negative_sentiment_label").rdd.mapPartitions(p => {
      p.map{
        case Row(text: String, negative_sentiment_label:String) => (text.split(" ").toSeq.map(_.trim).filter(_ != ""), negative_sentiment_label);
      };

    }).toDF("lemmas", "negative_sentiment_label");
    
    val lemm_word = new PrintWriter(new File("/home/hadoop/script_project/ML/send/lemm_word.txt"))
    lemm_word.write("true")
    lemm_word.close()

    // Remove Stop Words from the sequence of Lemmas
    val stopwords = sc.textFile("hdfs://master:9000/dz/cerist/pgs/bigdata/raw/list.txt").collect()
    val stopWordsRemovedDF = PreProcessorUtils.stopWordRemover(lemmatizedDF, "lemmas", "filtered_lemmas",stopwords)
                                .where(size(col("filtered_lemmas")) > 1);
    val stop_word = new PrintWriter(new File("/home/hadoop/script_project/ML/send/stop_word.txt"))
    stop_word.write("true")
    stop_word.close()

    /********************************************************************
     * SCALED FEATURE VECTOR
     ********************************************************************/

    // Generate the Scaled Feature Vectors
    val featurizedDF = ModelUtils.tfidf(stopWordsRemovedDF, "filtered_lemmas", "features");
    featurizedDF.cache();
    /********************************************************************
     * TRAIN AND EVALUATE A DECISION TREE CLASSIFIER
     ********************************************************************/
     val featurize = new PrintWriter(new File("/home/hadoop/script_project/ML/send/featurize.txt"))
    featurize.write("true")
    featurize.close()

    // Split the data into Training and Test Datasets
    val Array(trainingDF, testDF) = featurizedDF.randomSplit(Array(0.7, 0.3))
    trainingDF.cache();
    // Train a LogisticRegressionModel using the Training Dataset
    val (pipeline, lrm) = ModelUtils.trainLogisticRegressionModel(featurizedDF, trainingDF, "negative_sentiment_label", "features");
    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // With  2 values for lr.regParam,
    // this grid will have 2 parameter settings for CrossValidator to choose from.
    val paramGrid = new ParamGridBuilder()
                            .addGrid(lrm.regParam, Array(0.3, 0.2, 0.1, 0.05, 0.01))
                            .build()

    // Compute the accuracy of the LogisticRegression Training Model on the Test Dataset
    val logisticRegressionEvaluator = new MulticlassClassificationEvaluator()
                            .setLabelCol("indexed_label")
                            .setPredictionCol("prediction")
                            .setMetricName("accuracy");

    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    val cv = new CrossValidator()
                            .setEstimator(pipeline)
                            .setEvaluator(logisticRegressionEvaluator)
                            .setEstimatorParamMaps(paramGrid)
                            .setNumFolds(2)

    // Run cross-validation, and choose the best set of parameters.
    val logisticRegressionModel = cv.fit(trainingDF)
    
    // Afficher les valeurs AUC obtenues pour les combinaisons de la grille
    val arraycv = new PrintWriter(new File("/home/hadoop/script_project/ML/send/arraycv.txt"))
    arraycv.write(logisticRegressionModel.avgMetrics.mkString(", "))
    arraycv.close()

    // Afficher les meilleures valeurs pour les hyperparamètres
    val paramcv = new PrintWriter(new File("/home/hadoop/script_project/ML/send/paramcv.txt"))
    paramcv.write(logisticRegressionModel.getEstimatorParamMaps.zip(logisticRegressionModel.avgMetrics).maxBy(_._2)._1.toString())
    paramcv.close()

    // Afficher les meilleures valeurs pour les accuracys
    val bestcv = new PrintWriter(new File("/home/hadoop/script_project/ML/send/bestcv.txt"))
    bestcv.write(String.valueOf(logisticRegressionModel.getEstimatorParamMaps.zip(logisticRegressionModel.avgMetrics).maxBy(_._2)._2))
    bestcv.close()

    // Apply the LogisticRegression Training Model ti the Dataset trainingDF
    val logisticRegressionPredictionstr = logisticRegressionModel.transform(trainingDF);
    
    // Apply the LogisticRegression Training Model ti the Dataset testDF
    val logisticRegressionPredictionsts = logisticRegressionModel.transform(testDF);
    
    // Afficher 10 lignes complètes de logisticRegressionPredictionstest (sans la colonne features)
    logisticRegressionPredictionsts.select("negative_sentiment_label", "rawPrediction", "prediction").show(10, false);

    
    val logisticRegressionAccuracy = logisticRegressionEvaluator.evaluate(logisticRegressionPredictionsts);

    val accuracy = new PrintWriter(new File("/home/hadoop/script_project/ML/send/accuracy.txt"))
    accuracy.write("LogisticRegression Test Accuracy Rate = " + logisticRegressionAccuracy)
    accuracy.close()

    // Generate a Classification Matrix LogisticRegression
    val metrics = ModelUtils.generateMulticlassMetrics(logisticRegressionPredictionsts, "prediction", "indexed_label");
    println(metrics.confusionMatrix);

    val metric = new PrintWriter(new File("/home/hadoop/script_project/ML/send/metric.txt"))
    metric.write(metrics.confusionMatrix.toString().replace('\n',' '))
    metric.close()

    // Generate Label Accuracy Metrics LogisticRegression
    val labelMetrics = metrics.labels;
    labelMetrics.foreach { l =>
      println(s"False Positive Rate ($l) = " + metrics.falsePositiveRate(l));
    }
    

    /********************************************************************
     * SAVE THE LOGISTICREGRESSION CLASSIFIER FOR REAL-TIME STREAMING
     ********************************************************************/

    logisticRegressionModel.save("hdfs://master:9000/dz/cerist/pgs/bigdata/spark/"+mod);
    
    val savemodel = new PrintWriter(new File("/home/hadoop/script_project/ML/send/savemodel.txt"))
    savemodel.write("true")
    savemodel.close()

    val fin = new PrintWriter(new File("/home/hadoop/script_project/ML/send/fin.txt"))
    fin.write("true")
    fin.close()
  }

}
