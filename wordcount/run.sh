#!/bin/bash

OUT_DIR="word_count_had2020008"
mvn package
hdfs dfs -rm -r -skipTrash ${OUT_DIR}/*
yarn jar target/wordcount-1.0-SNAPSHOT.jar edu.phystech.wordcount.WordCount /data/wiki/en_articles ${OUT_DIR}
