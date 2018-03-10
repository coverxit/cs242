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

    @Override
    public void run() {
        LocalDateTime startAt = LocalDateTime.now();
        System.out.println("DocumentLengthImportThread started at " + startAt.toLocalTime() + ".");

        int indexedCount = 0;

        try (BufferedReader dataReader = new BufferedReader(
                new FileReader(Paths.get(jsonOutputPath, "data.json").toString()))) {

            BigInteger totalDocLength = BigInteger.ZERO;

            String dataLine;
            while ((dataLine = dataReader.readLine()) != null) {
                try {
                    JSONObject dataJson = new JSONObject(dataLine);
                    int length = new StringTokenizer(dataJson.getString("content")).countTokens();

                    // <docId, length>
                    database.put(JniDBFactory.bytes("__docLength_" + dataJson.getInt("id")),
                            JniDBFactory.bytes(String.valueOf(length)));

                    totalDocLength = totalDocLength.add(BigInteger.valueOf(length));
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

            String averageDocLength = totalDocLength.divide(BigInteger.valueOf(indexedCount)).toString();
            database.put(JniDBFactory.bytes("__avgDocLength"), JniDBFactory.bytes(averageDocLength));

            System.out.format("Summary: DocumentLengthImportThread has imported %d pages. Elapsed time: %s.%n",
                    indexedCount, Utility.elapsedTime(startAt, LocalDateTime.now()));
            System.out.println("Summary: Total document length is " + totalDocLength.toString() +
                    ", average document length is " + averageDocLength + ".");
        } catch (FileNotFoundException e) {
            System.out.println("DocumentLengthImportThread throws a FileNotFoundException.");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("DocumentLengthImportThread throws an IOException.");
            e.printStackTrace();
        }
    }
}
