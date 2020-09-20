package dz.cerist.pgs.bigdata.spark.ml;

import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions._;
import org.apache.spark.sql.types._;
import java.io.Serializable
import org.apache.spark.ml.feature.StopWordsRemover;

import scala.collection.JavaConversions._;
import scala.collection.mutable.ArrayBuffer;

/**
 * Pre-Processor Utilities / Helper Functions
 * A collection of functions to pre-process text
 *
 * @author jillur.quddus
 * @version 0.0.1
 */

object PreProcessorUtils extends Serializable{

  /**
   * Lowercase and Remove Punctuation
   * Lowercase and remove non-alphanumeric-space characters from the text field
   *
   * @param corpus The collection of documents as a Dataframe
   * @param textColumn The name of the column containing the text to be pre-processed
   * @return A Dataframe with the text lowercased and non-alphanumeric-space characters removed
   *
   */

  def lowercaseRemovePunctuation(corpus:Dataset[Row], textColumn:String): Dataset[Row] = {
    return corpus.withColumn(textColumn, regexp_replace(lower(corpus(textColumn)), "[.]|[!?]+|[!\u061F]+", ""));
  }

  /**
   * Text Lemmatizer
   * Given a text string, generate a sequence of Lemmas
   *
   * @param text The text string to lemmatize
   * @param pipeline The Stanford Core NLP Pipeline
   * @return A sequence of lemmas
   *
   */

  def lemmatizeText(text: String, pipeline:StanfordCoreNLP): Seq[String] = {

    val doc = new Annotation(text);
    pipeline.annotate(doc);
    val lemmas = new ArrayBuffer[String]();
    val sentences = doc.get(classOf[SentencesAnnotation]);
    for (sentence <- sentences; token <- sentence.get(classOf[TokensAnnotation])) {
      val lemma = token.get(classOf[LemmaAnnotation])
      if (lemma.length > 2 && isOnlyLetters(lemma)) {
        lemmas += lemma.toLowerCase
      }
    }

    return lemmas;

  }

  /**
   * Check that a given String is made up entirely of alpha characters
   *
   * @param str The string to test for alpha characters
   * @return Boolean
   */

  def isOnlyLetters(str: String) = str.forall(c => Character.isLetter(c));

  /**
   * Stop Words Remover
   * Remove Stop Words from a given input column containing a sequence of String
   *
   * @param corpus The collection of documents as a Dataframe
   * @param inputColumn The name of the column containing a sequence of Strings to filter
   * @param outputColumn The name of the column to output the filtered sequence of Strings to
   */

  def stopWordRemover(corpus:Dataset[Row], inputColumn:String, outputColumn:String, stopwords:Array[String]): Dataset[Row] = {
    val stopWordsRemover = new StopWordsRemover()
                             .setStopWords(stopwords)
                             .setInputCol(inputColumn)
                             .setOutputCol(outputColumn);
    return stopWordsRemover.transform(corpus);

  }

}
