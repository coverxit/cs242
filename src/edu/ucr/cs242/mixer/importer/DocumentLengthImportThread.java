package edu.ucr.cs242.mixer.importer;

import edu.ucr.cs242.Utility;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.json.JSONObject;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.StringTokenizer;

public class DocumentLengthImportThread extends Thread {
    private final DB database;
    private final String jsonOutputPath;

    /**
     * Construct a document length import thread, with given settings.
     * @param database       The LevelDB object.
     * @param jsonOutputPath The folder to the JSON output.
     */
    public DocumentLengthImportThread(DB database, String jsonOutputPath) {
        this.database = database;
        this.jsonOutputPath = jsonOutputPath;
    }

    private int putLength(JSONObject json, int fieldId, String fieldName) {
        int length = new StringTokenizer(json.getString(fieldName)).countTokens();

        // <docId, length>
        database.put(JniDBFactory.bytes("__docLength_" + json.getInt("id") + "_" + fieldId),
                JniDBFactory.bytes(String.valueOf(length)));

        return length;
    }

    @Override
    public void run() {
        LocalDateTime startAt = LocalDateTime.now();
        System.out.println("DocumentLengthImportThread started at " + startAt.toLocalTime() + ".");

        int indexedCount = 0;

        try (BufferedReader dataReader = new BufferedReader(
                new FileReader(Paths.get(jsonOutputPath, "data.json").toString()))) {

            // 0 - title, 1 - content, 2 - categories
            BigInteger[] totalDocLength = { BigInteger.ZERO, BigInteger.ZERO, BigInteger.ZERO };

            String dataLine;
            while ((dataLine = dataReader.readLine()) != null) {
                try {
                    JSONObject dataJson = new JSONObject(dataLine);

                    totalDocLength[0] = totalDocLength[0].add(BigInteger.valueOf(putLength(dataJson, 0, "title")));
                    totalDocLength[1] = totalDocLength[1].add(BigInteger.valueOf(putLength(dataJson, 1, "content")));
                    totalDocLength[2] = totalDocLength[2].add(BigInteger.valueOf(putLength(dataJson, 2, "categories")));

                    ++indexedCount;
                    if (indexedCount % 1000 == 0) {
                        System.out.format("DocumentLengthImportThread has imported %d pages. Elapsed time: %s.%n",
                                indexedCount, Utility.elapsedTime(startAt, LocalDateTime.now()));
                    }
                } catch (Exception e) {
                    System.out.println("DocumentLengthImportThread throws an Exception.");
                    e.printStackTrace();
                }
            }

            for (int i = 0; i < totalDocLength.length; i++) {
                String averageDocLength = totalDocLength[i].divide(BigInteger.valueOf(indexedCount)).toString();
                database.put(JniDBFactory.bytes("__avgDocLength_" + i), JniDBFactory.bytes(averageDocLength));
                System.out.println("Summary: Average document length for field " + i + " is " + averageDocLength +
                        " (total: " + totalDocLength[i].toString() + ").");
            }

            database.put(JniDBFactory.bytes("__docCount"), JniDBFactory.bytes(String.valueOf(indexedCount)));
            System.out.format("Summary: DocumentLengthImportThread has imported %d pages. Elapsed time: %s.%n",
                    indexedCount, Utility.elapsedTime(startAt, LocalDateTime.now()));
        } catch (FileNotFoundException e) {
            System.out.println("DocumentLengthImportThread throws a FileNotFoundException.");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("DocumentLengthImportThread throws an IOException.");
            e.printStackTrace();
        }
    }
}
