package edu.ucr.cs242.mapreduce;

import edu.ucr.cs242.Utility;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class IndexImportThread extends Thread {
    private final DB database;
    private final String jsonOutputPath;
    private final String hadoopIndexOutputPath;

    /**
     * Construct a index importer thread, with given settings.
     * @param database              The LevelDB object.
     * @param jsonOutputPath        The folder to the JSON output.
     * @param hadoopIndexOutputPath The file name to the Hadoop's index output.
     */
    public IndexImportThread(DB database, String jsonOutputPath, String hadoopIndexOutputPath) {
        this.database = database;
        this.jsonOutputPath = jsonOutputPath;
        this.hadoopIndexOutputPath = hadoopIndexOutputPath;
    }

    private void processDataLine(String dataLine) {
        String[] data = dataLine.split("\t");
        String keyword = data[0];

        JSONArray value = new JSONArray();
        Arrays.stream(data[1].split(";")).forEach(compact -> {
            String[] index = compact.split(":");
            String docId = index[0];

            String[] freqPos = index[1].split(Pattern.quote("|"));
            List<Integer> frequency = Arrays.stream(freqPos[0].split(","))
                    .map(Integer::parseInt).collect(Collectors.toList());

            String[] pos = freqPos[1].split(",");
            List<List<Integer>> position = new ArrayList<>();

            for (int count = 0, i = 0; i < frequency.size(); i++) {
                int freq = frequency.get(i);
                position.add(Arrays.stream(pos).skip(count).limit(freq)
                        .map(Integer::parseInt).collect(Collectors.toList()));
                count += freq;
            }

            value.put(new JSONObject()
                    .put(docId, new JSONObject()
                            .put("frequency", frequency)
                            .put("position", position)
            ));
        });

        database.put(JniDBFactory.bytes(keyword), JniDBFactory.bytes(value.toString()));
    }

    @Override
    public void run() {
        LocalDateTime startAt = LocalDateTime.now();
        System.out.println("IndexImportThread started at " + startAt.toLocalTime() + ".");

        int indexedCount = 0;

        try (BufferedReader indexReader = new BufferedReader(
                new FileReader(Paths.get(jsonOutputPath, "index.json").toString()));
             BufferedReader dataReader = new BufferedReader(
                     new FileReader(new File(hadoopIndexOutputPath)))) {

            String indexLine, dataLine;
            while ((indexLine = indexReader.readLine()) != null && (dataLine = dataReader.readLine()) != null) {
                JSONObject indexJson = new JSONObject(indexLine);

                // <docId,title>
                database.put(JniDBFactory.bytes("__docId_" + indexJson.getInt("id")),
                        JniDBFactory.bytes(indexJson.getString("title")));

                processDataLine(dataLine);

                ++indexedCount;
                if (indexedCount % 1000 == 0) {
                    System.out.format("IndexImportThread has imported %d pages. Elapsed time: %s.%n",
                            indexedCount, Utility.elapsedTime(startAt, LocalDateTime.now()));
                }
            }

            System.out.format("Summary: IndexImportThread has imported %d pages. Elapsed time: %s.%n",
                    indexedCount, Utility.elapsedTime(startAt, LocalDateTime.now()));
        } catch (FileNotFoundException e) {
            System.out.println("IndexImportThread throws a FileNotFoundException.");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("IndexImportThread throws an IOException.");
            e.printStackTrace();
        }
    }
}
