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
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

class IndexMapper extends Mapper<Object, Text, Text, Text> {
    private final List<String> stopWords = Arrays.asList(
            "a", "an", "and", "are", "as", "at", "be", "but", "by",
            "for", "if", "in", "into", "is", "it",
            "no", "not", "of", "on", "or", "such",
            "that", "the", "their", "then", "there", "these",
            "they", "this", "to", "was", "will", "with"
    );

    private void mapInvertedIndex(String pageTitle, String field, String value, Context context)
            throws IOException, InterruptedException {

        Map<String, Integer> frequency = new HashMap<>();
        Map<String, List<Integer>> position = new HashMap<>();
        StringTokenizer tokenizer = new StringTokenizer(value);

        int tokenCount = 0;
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken()
                    // Remove the beginning and ending punctuation
                    .replaceAll("^\\p{Punct}*|\\p{Punct}*$", "")
                    // Ensure lower case
                    .trim().toLowerCase();

            // We only index alphanumeric and non-empty words
            if (Pattern.matches("^[\\p{Alnum}]+$", token)) {
                if (!stopWords.contains(token)) {
                    if (!frequency.containsKey(token)) {
                        frequency.put(token, 0);
                        position.put(token, new ArrayList<>());
                    }

                    frequency.put(token, frequency.get(token) + 1);
                    position.get(token).add(tokenCount);
                }
            }

            ++tokenCount;
        }

        for (Map.Entry<String, Integer> entry : frequency.entrySet()) {
            JSONObject element = new JSONObject();
            element.put("field", field);
            element.put("frequency", entry.getValue());
            element.put("position", position.get(entry.getKey()));
            context.write(new Text(entry.getKey() + "|" + pageTitle), new Text(element.toString()));
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        try {
            JSONObject json = new JSONObject(value.toString());

            // We index in lowercase
            String title = json.getString("title").toLowerCase();
            String content = json.getString("content").toLowerCase();
            String categories = json.getJSONArray("categories").toList().stream()
                    .map(Objects::toString).map(String::toLowerCase)
                    .collect(Collectors.joining(" "));

            // Output a pair of <keyword|title, {field: "content", frequency: 1, position: [5]}>
            mapInvertedIndex(title, "title", title, context);
            mapInvertedIndex(title, "content", content, context);
            mapInvertedIndex(title, "categories", categories, context);
        } catch (JSONException e) {
            // The last line of input file (the empty line), will trigger this exception.
            // But maybe possible some other problem occurred
            if (!value.toString().isEmpty()) {
                System.out.println("JSONException, with value of `" + value.toString() + "`");
                e.printStackTrace();
            }
        }
    }
}

class IndexReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String[] keySet = key.toString().split(Pattern.quote("|"));
        String keyword = keySet[0], pageTitle = keySet[1];

        JSONObject json = new JSONObject();
        json.put("title", pageTitle);
        // In a format of
        // [{field: "content", frequency: 1, position: [5]}, {field: "title", frequency: 1, position: [3]}]
        json.put("index", StreamSupport.stream(values.spliterator(), false)
                .map(Text::toString).map(JSONObject::new).collect(Collectors.toList()));

        // Output a pair of <keyword, {title: "Beijing Subway", index: the above array}>
        context.write(new Text(keyword), new Text(json.toString()));
    }
}

public class MapReduce {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("usage: mapreduce <json-data-input-path> <json-index-output-path>");
        } else {
            System.out.println("MapReduce started at " + LocalDateTime.now().toLocalTime() + ". ");

            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "MapReduceIndexer");
            job.setJarByClass(MapReduce.class);

            job.setMapperClass(IndexMapper.class);
            job.setReducerClass(IndexReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
}
