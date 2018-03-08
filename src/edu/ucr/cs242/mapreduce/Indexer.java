package edu.ucr.cs242.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

class IndexMapper extends Mapper<Object, Text, Text, Text> {
    private final List<String> stopWords = Arrays.asList(
            "a", "an", "and", "are", "as", "at", "be", "but", "by",
            "for", "if", "in", "into", "is", "it",
            "no", "not", "of", "on", "or", "such",
            "that", "the", "their", "then", "there", "these",
            "they", "this", "to", "was", "will", "with"
    );

    private JSONObject createInvertedIndex(String text) {
        Map<String, Integer> freq = new HashMap<>();
        Map<String, List<Integer>> pos = new HashMap<>();
        StringTokenizer tokenizer = new StringTokenizer(text);

        int tokenCount = 0;
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();

            if (!stopWords.contains(token)) {
                if (!freq.containsKey(token)) {
                    freq.put(token, 0);
                    pos.put(token, new ArrayList<>());
                }

                freq.put(token, freq.get(token) + 1);
                pos.get(token).add(tokenCount);
            }

            ++tokenCount;
        }

        JSONObject ret = new JSONObject();
        freq.forEach((k, v) -> {
            JSONObject el = new JSONObject();
            el.put("freq", v);
            el.put("pos", pos.get(k));
            ret.put(k, el);
        });
        return ret;
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        JSONObject json = new JSONObject(value.toString());

        String title = json.getString("title").toLowerCase();
        String content = json.getString("content").toLowerCase();
        String categories = json.getJSONArray("categories").toList().stream()
                .map(Objects::toString).collect(Collectors.joining(" "));

        JSONObject map = new JSONObject();
        map.put("title", createInvertedIndex(title));
        map.put("content", createInvertedIndex(content));
        map.put("categories", createInvertedIndex(categories));



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
            System.out.println("Indexer started at " + LocalDateTime.now().toLocalTime() + ". ");

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
