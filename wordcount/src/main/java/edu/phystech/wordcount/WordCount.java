package edu.phystech.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class WordCount extends Configured implements Tool {
    public static class SplitMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final Text key = new Text();
        private final LongWritable count = new LongWritable(1);

        public void map(LongWritable basic_key, Text line, Context context) throws IOException, InterruptedException {
            for (String word : line.toString().split("\\s+")) {
                key.set(word);
                context.write(key, count);
            }
        }
    }

    public static class CountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private final LongWritable count = new LongWritable();


        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long summaryCount = 0;
            for (LongWritable count : values) {
                summaryCount += count.get();
            }

            count.set(summaryCount);
            context.write(key, count);
        }
    }

    public static class ReorderMapper extends Mapper<Text, LongWritable, LongWritable, Text> {

        public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
            context.write(value, key);
        }
    }

    public static class ReorderReducer extends Reducer<LongWritable, Text, Text, LongWritable> {

        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text text : values) {
                context.write(text, key);
            }
        }
    }

    public static class InverseLongComparator extends WritableComparator {

        protected InverseLongComparator() {
            super(LongWritable.class, true);
        }

        @Override
        public int compare(WritableComparable raw_lhs, WritableComparable raw_rhs) {
            LongWritable lhs = (LongWritable) raw_lhs;
            LongWritable rhs = (LongWritable) raw_rhs;

            return -1 * lhs.compareTo(rhs);
        }
    }


    public static void main(String[] args) throws Exception {
        ToolRunner.run(new WordCount(), args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();


        Job countJob = Job.getInstance(conf);
        countJob.setJarByClass(WordCount.class);
        countJob.setJobName("word count: 1st job");

        countJob.setMapperClass(SplitMapper.class);
        countJob.setMapOutputKeyClass(Text.class);
        countJob.setMapOutputValueClass(LongWritable.class);

        countJob.setReducerClass(CountReducer.class);
        countJob.setOutputKeyClass(Text.class);
        countJob.setOutputValueClass(LongWritable.class);
        countJob.setNumReduceTasks(10);

        countJob.setInputFormatClass(TextInputFormat.class);
        countJob.setOutputFormatClass(SequenceFileOutputFormat.class);

        TextInputFormat.addInputPath(countJob, new Path(strings[0]));
        SequenceFileOutputFormat.setOutputPath(countJob, new Path(strings[1] + "_tmp"));


        Job sortJob = Job.getInstance(conf);
        sortJob.setJarByClass(WordCount.class);
        sortJob.setJobName("word count: 2nd job");

        sortJob.setMapperClass(ReorderMapper.class);
        sortJob.setMapOutputKeyClass(LongWritable.class);
        sortJob.setMapOutputValueClass(Text.class);

        sortJob.setSortComparatorClass(InverseLongComparator.class);

        sortJob.setReducerClass(ReorderReducer.class);
        sortJob.setOutputKeyClass(Text.class);
        sortJob.setOutputValueClass(LongWritable.class);
        sortJob.setNumReduceTasks(1);

        sortJob.setInputFormatClass(SequenceFileInputFormat.class);
        sortJob.setOutputFormatClass(TextOutputFormat.class);

        SequenceFileInputFormat.addInputPath(sortJob, new Path(strings[1] + "_tmp"));
        TextOutputFormat.setOutputPath(sortJob, new Path(strings[1]));


        if (!countJob.waitForCompletion(true)) {
            return 1;
        }

        return sortJob.waitForCompletion(true) ? 0 : 1;
    }
}
