package edu.phystech.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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
            for (String word : line.toString().split(" ")) {
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

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new WordCount(), args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();


        Job countJob = Job.getInstance(conf);
        countJob.setJarByClass(WordCount.class);

        countJob.setMapperClass(SplitMapper.class);
        countJob.setMapOutputKeyClass(Text.class);
        countJob.setMapOutputValueClass(LongWritable.class);

        countJob.setReducerClass(CountReducer.class);
        countJob.setOutputKeyClass(Text.class);
        countJob.setOutputValueClass(LongWritable.class);
        countJob.setNumReduceTasks(9);

        countJob.setInputFormatClass(TextInputFormat.class);
        countJob.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(countJob, new Path(strings[0]));
        TextOutputFormat.setOutputPath(countJob, new Path(strings[1]));

        return countJob.waitForCompletion(true) ? 0 : 1;
    }
}
