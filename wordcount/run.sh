#!/bin/bash

OUT_DIR="word_count"
mvn package
hdfs dfs -rm -r -skipTrash ${OUT_DIR}*
yarn jar target/PageLifeCounter-1.0.jar org.atp.PageLifeCounter /data/wiki/en_articles ${OUT_DIR}
