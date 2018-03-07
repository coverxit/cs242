package edu.ucr.cs242.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.time.LocalDateTime;

class IndexMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
    }
}

class IndexReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

    }
}

public class Indexer {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("usage: indexer <input path> <output path>");
        } else {
            int numOfPages = 0;
            System.out.println("Indexer started at " + LocalDateTime.now().toLocalTime() + ". " +
                    "Pages to index: " + numOfPages + ".");

            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "MapReduceIndexer");
            job.setJarByClass(Indexer.class);

            job.setMapperClass(IndexMapper.class);
            job.setReducerClass(IndexReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
}
