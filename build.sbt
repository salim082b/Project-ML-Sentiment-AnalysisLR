name := "Twitter_Sen_Ana_ML_LR"

version := "0.1"

scalaVersion := "2.11.8"

resolvers ++= Seq(
        "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases",
        "JBoss" at "https://repository.jboss.org/nexus/content/repositories/thirdparty-releases/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming-twitter" % "1.6.3" exclude("org.twitter4j", "twitter4j"),
  "org.apache.spark" %% "spark-core" % "1.6.3",
  "org.apache.spark" %% "spark-sql" % "1.6.3",
  "org.apache.spark" %% "spark-mllib" % "2.2.0",
  "org.apache.spark" %% "spark-streaming" % "1.6.3" % "provided",
  "org.twitter4j" % "twitter4j-core" % "4.0.6",
  "org.twitter4j" % "twitter4j-stream" % "4.0.6",
  "edu.stanford.nlp" % "stanford-corenlp" % "4.0.0",
  "edu.stanford.nlp" % "stanford-parser" % "3.9.2",
  "edu.stanford.nlp" % "stanford-corenlp" % "4.0.0"  classifier "models-arabic",
  "org.elasticsearch" % "elasticsearch-hadoop" % "6.4.2",
  "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.1.0",
  "com.twitter" %% "bijection-avro" % "0.9.5",
  "org.apache.avro" % "avro" % "1.8.1",
  "com.databricks" %% "spark-csv" % "1.5.0",
  "commons-logging" % "commons-logging" % "1.2",
  "com.google.code.geocoder-java" % "geocoder-java" % "0.16"  
)

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api"       % "1.7.25",
  "org.slf4j" % "jcl-over-slf4j"  % "1.7.25"
).map(_.force())

libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-jdk14")) }
