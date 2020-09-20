#!/bin/bash

url=$(</home/hadoop/script_project/SA/receive/mlmodel.txt)
has=$(</home/hadoop/script_project/SA/receive/hashtage.txt)
ind=$(</home/hadoop/script_project/SA/receive/indexes.txt)
kib=$(</home/hadoop/script_project/SA/receive/linkkibana.txt)

~/spark/bin/spark-submit --class   dz.cerist.pgs.bigdata.spark.ml.Twitter_ML_LR_CV_Analyse_Sentiment  --master local[10] --packages "org.apache.spark:spark-streaming-twitter_2.11:1.6.3","org.elasticsearch:elasticsearch-spark-20_2.11:6.4.2","databricks:spark-corenlp:0.2.0-s_2.11,edu.stanford.nlp:stanford-corenlp:4.0.0","org.twitter4j:twitter4j-core:4.0.4","org.twitter4j:twitter4j-stream:4.0.4","org.apache.spark:spark-core_2.11:1.5.2"  --jars /home/hadoop/.ivy2/cache/edu.stanford.nlp/stanford-corenlp/jars/stanford-corenlp-4.0.0-models-arabic.jar,/home/hadoop/twitter4j-core-4.0.4.jar,/home/hadoop/twitter4j-stream-4.0.4.jar,/home/hadoop/spark-streaming-twitter_2.11-1.6.3.jar,/home/hadoop/08ProjectTSALR/spark-core_2.11-1.5.2.logging.jar /home/hadoop/08ProjectTSALR/target/scala-2.11/twitter_sen_ana_ml_lr_2.11-0.1.jar $url $has $ind $kib consumerKey consumerSecret accessToken accessTokenSecret yoursearchTag  dfsadmin -safemode leave dfsadmin -safemode leave

