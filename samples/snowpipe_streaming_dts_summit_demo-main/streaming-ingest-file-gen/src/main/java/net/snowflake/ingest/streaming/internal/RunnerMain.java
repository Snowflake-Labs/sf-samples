/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;

/**
 * Example on how to use the Streaming Ingest client APIs.
 *
 * <p>Please read the README.md file for detailed steps
 */
public class RunnerMain {
  // Please follow the example in profile_streaming.json.example to see the required properties, or
  // if you have already set up profile.json with Snowpipe before, all you need is to add the "role"
  // property.
  private static String PROFILE_PATH =
      "/Users/tjones/projects/snowpipe_streaming_dts_summit_demo/profile.json";
  private static final ObjectMapper mapper = new ObjectMapper();

  private static Logger LOGGER = Logger.getLogger("MyLog");

  private static void configureLogger() {
    FileHandler fh;
    try {
      fh = new FileHandler(RunnerMain.class.getName());
      LOGGER.addHandler(fh);
      SimpleFormatter formatter = new SimpleFormatter();
      fh.setFormatter(formatter);
      fh.setLevel(Level.ALL);
    } catch (SecurityException | IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws Exception {
    configureLogger();

    Properties props = new Properties();
    Iterator<Map.Entry<String, JsonNode>> propIt =
        mapper.readTree(new String(Files.readAllBytes(Paths.get(PROFILE_PATH)))).fields();
    while (propIt.hasNext()) {
      Map.Entry<String, JsonNode> prop = propIt.next();
      props.put(prop.getKey(), prop.getValue().asText());
      System.out.println(prop.getValue().asText());
    }

    // Create a streaming ingest client, open channel to table
    try (SnowflakeStreamingIngestClient client =
        SnowflakeStreamingIngestClientFactory.builder("SUMMIT_CLIENT")
            .setProperties(props)
            .build()) {
      OpenChannelRequest request =
          OpenChannelRequest.builder("MY_CHANNEL")
              .setDBName("TJONESDB")
              .setSchemaName("SUMMIT_SCH")
              .setTableName("LANDING_WORDS_T")
              .setOnErrorOption(
                  OpenChannelRequest.OnErrorOption.CONTINUE) // Another ON_ERROR option is ABORT
              .build();

      // Open a streaming ingest channel from the given client
      SnowflakeStreamingIngestChannel channel = client.openChannel(request);

      // Start reading input
      try {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
          String line;
          while ((line = in.readLine()) != null) {
            String[] words = line.split(" ");
            for (String word : words) {
              Map<String, Object> row = new HashMap<>();
              row.put("user_id", 1);
              row.put("word", word);
              InsertValidationResponse response = channel.insertRow(row, null);
              if (response.hasErrors()) {
                throw response.getInsertErrors().get(0).getException();
              }
            }
          }
        }
      } catch (Exception e) {
        LOGGER.info("Caught exception: " + e.getMessage());
      }
    }
  }
}
