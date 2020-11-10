#!/bin/bash

IN_DIR="/data/wiki/en_articles"
OUT_DIR="word_count"

JAR_NAME="target/wordcount-1.0-SNAPSHOT.jar"
MAIN_CLASS="edu.phystech.wordcount.WordCount"

mvn package

hdfs dfs -rm -r -skipTrash ${OUT_DIR}*
yarn jar ${JAR_NAME} ${MAIN_CLASS} ${IN_DIR} ${OUT_DIR}

