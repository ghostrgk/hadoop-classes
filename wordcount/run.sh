#!/bin/bash

IN_DIR="/data/wiki/en_articles_part"
OUT_DIR="word_count_had2020008"

JAR_NAME="target/wordcount-1.0-SNAPSHOT.jar"
MAIN_CLASS="edu.phystech.wordcount.WordCount"

mvn package

hdfs dfs -rm -r -skipTrash ${OUT_DIR}*
yarn jar ${JAR_NAME} ${MAIN_CLASS} ${IN_DIR} ${OUT_DIR}

