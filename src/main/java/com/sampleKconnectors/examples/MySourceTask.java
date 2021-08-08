package com.sampleKconnectors.examples;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

//https://www.arm64.ca/post/reading-parquet-files-java/

//3rd party Library Imports
import org.jsoup.*;
import org.jsoup.Jsoup;
import org.jsoup.Connection;



public class MySourceTask extends SourceTask {
  static final Logger log = LoggerFactory.getLogger(MySourceTask.class);
  private String urlToAnalyze;
  private String outPutTopicName;
  private Map sourcePartition;
  private Connection connection ;

  @Override
  public String version() {

    return VersionUtil.getVersion();
  }

  @Override
  public void start(final Map<String, String> props) {
    //TODO: Do things here that are required to start your task. This could be open a connection to a database, etc.
    System.out.println("MySourceConnector Property invoked" + "{" + props + "}");
    urlToAnalyze = props.get(MySourceConnector.URL_TO_ANALYZE);
    outPutTopicName = props.get(MySourceConnector.OUTPUT_TOPIC);
    sourcePartition = Collections.singletonMap("uriToAnalyze",urlToAnalyze);
    connection = Jsoup.connect(urlToAnalyze);
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    //TODO: Create SourceRecord objects that will be sent the kafka cluster.
    List<SourceRecord> result = new ArrayList<>();

    System.out.println("MySourceTask -> poll -> invoked!!");
    try {
      final Document httpDocument = connection.timeout(1000).get();
      final String localDateTime = LocalDateTime.now().toString();
      final Elements elements = httpDocument.select("a[href]");

      if(!elements.isEmpty()){
        for(int i=0;i<elements.size();i++){
          final Element element = elements.get(i);
          final Map sourceOffset = Collections.singletonMap("linkOffset",i);
          final SourceRecord record = new SourceRecord(sourcePartition,sourceOffset,outPutTopicName,
                  Schema.STRING_SCHEMA,localDateTime,Schema.STRING_SCHEMA,element.attr("href"));
          result.add(record);
        }
      }
      Thread.sleep(1000);
    }
    catch (IOException e){
      log.info("Something went wrong!");
      System.out.println("Something went wrong");
      e.printStackTrace();
    }
    catch (InterruptedException e){
      log.info("Something went wrong!!");
      System.out.println("Something went wrong!!");
      e.printStackTrace();
    }
    return result;
  }

  @Override
  public void stop() {
    //TODO: Do whatever is required to stop your task.
  }
}