package edu.ucr.cs242.webapi;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.ucr.cs242.Utility;
import org.apache.commons.cli.*;
import org.json.JSONObject;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class Server {
    private final int port;
    private final String jdbcUrl;
    private final Path luceneIndexPath;
    private HttpServer httpServer;

    class QueryHandler implements HttpHandler {
        private final String jdbcUrl;

        public QueryHandler(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
        }

        private void writeResponse(HttpExchange httpExchange, int httpStatusCode, JSONObject jsonObject) throws IOException {
            byte bytes[] = jsonObject.toString().getBytes("utf-8");
            httpExchange.sendResponseHeaders(httpStatusCode, bytes.length);
            OutputStream os = httpExchange.getResponseBody();
            os.write(bytes);
            os.close();
        }

        private void writeSuccess(HttpExchange httpExchange, JSONObject jsonObject) throws IOException {
            // HTTP 200: OK
            writeResponse(httpExchange, 200, new JSONObject().put("error", false).put("data", jsonObject));
        }

        private void writeFailure(HttpExchange httpExchange, String reason) throws IOException {
            // HTTP 400: Bad Request
            writeResponse(httpExchange, 400, new JSONObject().put("error", true).put("data", reason));
        }

        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            String rawQuery = httpExchange.getRequestURI().getRawQuery();
            httpExchange.getResponseHeaders().set("Content-Type", "application/json");

            if (rawQuery == null) {
                writeFailure(httpExchange, "Empty request");
            } else {
                // Split query by '&'
                Map<String, String> urlQuery = Arrays.stream(rawQuery.split("&"))
                        .map(param -> {
                            // Parameters are in the form of "key=value"
                            String pair[] = param.split("=");
                            if (pair.length == 2) {
                                try {
                                    String key = URLDecoder.decode(pair[0], "utf-8").trim();
                                    String value = URLDecoder.decode(pair[1], "utf-8").trim();
                                    return key.isEmpty() || value.isEmpty() ? null : new AbstractMap.SimpleEntry<>(key, value);
                                } catch (UnsupportedEncodingException e) {
                                    return null;
                                }
                            }

                            return null;
                        }).filter(Objects::nonNull)
                        .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

                // Process parameters
                if (!urlQuery.containsKey("method")) {
                    writeFailure(httpExchange, "Parameter `method` missing.");
                } else if (!urlQuery.containsKey("keyword")) {
                    writeFailure(httpExchange, "Parameter `keyword` missing.");
                } else {
                    String method = urlQuery.get("method").toLowerCase();
                    if (!method.equals("lucene") && !method.equals("mapreduce")) {
                        writeFailure(httpExchange, "Invalid parameter `method`. Available methods are `lucene` and `mapreduce`.");
                    } else {
                        String keyword = urlQuery.get("keyword");

                        try {
                            Searcher searcher;
                            if (method.equals("lucene")) {
                                searcher = new LuceneSearcher(jdbcUrl, luceneIndexPath);
                            } else {
                                searcher = null;
                            }

                            LocalDateTime start = LocalDateTime.now();
                            JSONObject searchResult = searcher.search(keyword);
                            LocalDateTime end = LocalDateTime.now();

                            writeSuccess(httpExchange, searchResult.put("elapsedTime", Duration.between(start, end).toMillis()));
                        } catch (SQLException e) {
                            System.out.println("Server throws an SQLException: " + e.getMessage());
                        }
                    }
                }
            }
        }
    }

    /**
     * Construct an RESTful API server with given settings.
     * @param port            The port to listen on.
     * @param jdbcUrl         The JDBC url to the database.
     * @param luceneIndexPath The directory to the Lucene index.
     */
    public Server(int port, String jdbcUrl, Path luceneIndexPath) {
        this.port = port;
        this.jdbcUrl = jdbcUrl;
        this.luceneIndexPath = luceneIndexPath;
    }

    public void start() {
        try {
            httpServer = HttpServer.create(new InetSocketAddress(port), 0);
            httpServer.createContext("/query", new QueryHandler(jdbcUrl));
            httpServer.setExecutor(null);
            httpServer.start();

            System.out.println("RESTful API server started (listening on " + port + ").");
            System.out.println("Press Ctrl+C to terminate.");
        } catch (IOException e) {
            System.out.println("Server throws an IOException: " + e.getMessage());
        }

        // Handle Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Stopping server...");
            httpServer.stop(0);
            System.out.println("Server stopped.");
        }));
    }

    private static void printMessage(String message) {
        System.out.println("server: " + message);
    }

    private static void printUsage() {
        System.out.println("usage: server [options] <jdbc-url> <lucene-index-path>");
        System.out.println("use -h for a list of possible options");
        System.exit(1);
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("server [options] <jdbc-url> <lucene-index-path>", options);
        System.out.println();
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        final int PORT = 10483;

        Options options = new Options();
        options.addOption(Option.builder("p")
                .longOpt("port")
                .argName("PORT")
                .desc("the port to listen on (default: " + PORT + ")")
                .numberOfArgs(1)
                .build());

        options.addOption("h", "help", false, "print a synopsis of standard options");

        try {
            CommandLine cmd = new DefaultParser().parse(options, args);
            List<String> argList = cmd.getArgList();

            if (cmd.hasOption("h")) {
                printHelp(options);
                System.exit(0);
            }

            if (argList.isEmpty()) {
                printMessage("JDBC url is not specified");
                printUsage();
            }

            if (argList.size() <= 1) {
                printMessage("Lucene index path is not specified");
                printUsage();
            }

            try {
                int port = Integer.parseInt(cmd.getOptionValue("port", String.valueOf(PORT)));

                String jdbcUrl = argList.get(0);
                Optional<Connection> dbConnection = Utility.getConnection(jdbcUrl);
                if (!dbConnection.isPresent()) {
                    printMessage("invalid JDBC url");
                    printUsage();
                } else {
                    Path luceneIndexPath = Paths.get(argList.get(1));
                    if (!Files.exists(luceneIndexPath) || !Files.isDirectory(luceneIndexPath)) {
                        printMessage("invalid Lucene index path (not exist or not directory)");
                        printUsage();
                    }

                    dbConnection.get().close();
                    new Server(port, jdbcUrl, luceneIndexPath).start();

                }
            } catch (NumberFormatException e) {
                printMessage("invalid option(s)");
                printHelp(options);
                System.exit(1);
            }
        } catch (ParseException e) {
            // Lower the first letter, which as default is an upper letter.
            printMessage(e.getMessage().substring(0, 1).toLowerCase() + e.getMessage().substring(1));
            printHelp(options);
            System.exit(1);
        }
    }
}
