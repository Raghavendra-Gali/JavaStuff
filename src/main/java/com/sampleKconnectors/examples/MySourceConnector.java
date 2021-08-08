package com.sampleKconnectors.examples;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
//import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySourceConnector extends SourceConnector {
  private String uriToAnalyze;
  private String outputTopicName;
  public static final String URL_TO_ANALYZE = "uri.to.analyze";
  public static final String OUTPUT_TOPIC = "output.topic.name" ;
  private static Logger log = LoggerFactory.getLogger(MySourceConnector.class);

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
          .define(URL_TO_ANALYZE, ConfigDef.Type.STRING,null,
                  ConfigDef.Importance.HIGH,"Required URL To analyze")
          .define(OUTPUT_TOPIC,ConfigDef.Type.STRING,
                  null,ConfigDef.Importance.HIGH,"Where To write");

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    System.out.println("STARTING THE CONNECTOR" + "{" + props + "}");
    uriToAnalyze = props.get(URL_TO_ANALYZE);
    outputTopicName = props.get(OUTPUT_TOPIC);

    //TODO: Add things you need to do to setup your connector.
  }

  @Override
  public Class<? extends Task> taskClass() {
    //TODO: Return your task implementation.
    return MySourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    //TODO: Define the individual task configurations that will be executed.
    ArrayList<Map<String ,String>> configs = new ArrayList<>();
    Map<String,String> config = new HashMap<>();
    config.put(URL_TO_ANALYZE,uriToAnalyze);
    config.put(OUTPUT_TOPIC,outputTopicName);
    configs.add(config);
    return  configs;

  }

  @Override
  public void stop() {
    //TODO: Do things that are necessary to stop your connector.
    System.out.println("STOP THE CONNECTOR!!!");
    log.info("STOP THE CONNECTOR!!");
  }

  @Override
  public ConfigDef config() {

    return CONFIG_DEF;
  }
}
