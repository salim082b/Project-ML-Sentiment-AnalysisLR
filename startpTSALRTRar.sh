#!/bin/bash

cd ~/08ProjectTSALR

url=$(</home/hadoop/script_project/ML/receive/dataset.txt)
mod=$(</home/hadoop/script_project/ML/receive/namemodel.txt)

~/spark/bin/spark-submit --class dz.cerist.pgs.bigdata.spark.ml.TrainLogisticRegressionClassifier --master local[4] --driver-memory 3g --executor-memory 3g --packages "org.apache.spark:spark-streaming-twitter_2.11:1.6.3","org.elasticsearch:elasticsearch-spark-20_2.10:6.7.0","databricks:spark-corenlp:0.2.0-s_2.11,edu.stanford.nlp:stanford-corenlp:4.0.0","org.twitter4j:twitter4j-core:4.0.4","org.twitter4j:twitter4j-stream:4.0.4","org.apache.spark:spark-core_2.11:1.5.2"  --jars /home/hadoop/.ivy2/cache/edu.stanford.nlp/stanford-corenlp/jars/stanford-corenlp-4.0.0-models-arabic.jar:/home/hadoop/.cache/coursier/v1/https/repo1.maven.org/maven2/org/apache/spark/spark-mllib_2.11/2.2.0/spark-mllib_2.11-2.2.0.jar /home/hadoop/08ProjectTSALR/target/scala-2.11/twitter_sen_ana_ml_lr_2.11-0.1.jar $url $mod consumerKey consumerSecret accessToken accessTokenSecret yoursearchTag  dfsadmin -safemode leave dfsadmin -safemode leave
