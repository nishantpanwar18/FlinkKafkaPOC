package org.example;


import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;


import static org.apache.flink.table.api.Expressions.*;
import static org.apache.flink.table.api.Tumble.over;

public class RealTimeViewEnricher {

    private static final String KAFKA_BROKER = "kafka:29092";
    private static final String TOPIC_NAME = "very_high_throughput_topic";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        new Thread(() -> {
            try {
                VeryHighThroughputProducer.main(new String[]{});
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        // 1. Ingest Data from Kafka
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BROKER)
                .setTopics(TOPIC_NAME)
                .setGroupId("flink-dashboard-consumer")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        DataStream<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Convert the JSON string stream to a Row DataStream
        DataStream<Row> eventsStream = kafkaStream.map(json -> {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(json);
            return Row.of(
                    node.get("user_id").asInt(),
                    node.get("event_type").asText(),
                    node.get("timestamp").asLong(),
                    node.get("value").asDouble()
            );
        }).returns(new org.apache.flink.api.java.typeutils.RowTypeInfo(
                org.apache.flink.api.common.typeinfo.Types.INT,
                org.apache.flink.api.common.typeinfo.Types.STRING,
                org.apache.flink.api.common.typeinfo.Types.LONG,
                org.apache.flink.api.common.typeinfo.Types.DOUBLE
        ));
        // Create the TableEnvironment
        StreamTableEnvironment t_env = StreamTableEnvironment.create(env);

        // Convert DataStream to a Table with a watermark
        Table eventsTable = t_env.fromDataStream(
                eventsStream,
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .column("f1", DataTypes.STRING())
                        .column("f2", DataTypes.BIGINT())
                        .column("f3", DataTypes.DOUBLE())
                        .columnByExpression("rowtime", "TO_TIMESTAMP_LTZ(f2, 3)")
                        .watermark("rowtime", "rowtime - INTERVAL '5' SECOND")
                        .build()

        ).as("user_id","event_type","timestamp","value","rowtime");



        // 4. Perform Windowed Aggregation on Enriched Data
        Table resultTable = eventsTable
                .window(over(lit(5).seconds()).on($("rowtime")).as("w"))
                .groupBy($("event_type"), $("w"))
                .select(
                        $("event_type"),
                        $("event_type").count().as("event_count"),
                        $("w").end().as("window_end")
                );



        DataStream<EventCount> resultStream = t_env
                .toDataStream(resultTable)
                .map(row -> {
                    EventCount eventCount = new EventCount();
                    eventCount.eventType = (String) row.getField("event_type");
                    eventCount.eventCount = (Long) row.getField("event_count");
                    eventCount.windowEnd = row.getField("window_end").toString();
                    return eventCount;
                });


//        resultStream.print();

        resultStream.addSink(new PrometheusPushGatewaySink("pushgateway:9091", "flink_windowed_job"));

        System.out.println();

        System.out.println("Submitting Flink job...");
        env.execute("Real-time Enrichment Job");
        System.out.println("Flink job execution started.");
    }
}
