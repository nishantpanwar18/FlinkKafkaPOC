package org.example;

import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.IOException;

public class PrometheusPushGatewaySink extends RichSinkFunction<EventCount> {

    private final String pushgatewayAddress;
    private final String jobName;
    private transient PushGateway pushGateway;

    // Define a Gauge metric outside of processElement to avoid creating it repeatedly
    private static final Gauge eventCounter = Gauge.build()
            .name("flink_windowed_job_abc")
            .help("Event count within a 5-second window")
            .labelNames("event_type")
            .create();

    public PrometheusPushGatewaySink(String pushgatewayAddress, String jobName) {
        this.pushgatewayAddress = pushgatewayAddress;
        this.jobName = jobName;
        eventCounter.register();
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // Initialize the PushGateway client inside the open() method
        // to avoid serialization issues
        pushGateway = new PushGateway(pushgatewayAddress);

        // Register the metric if it hasn't been registered yet
        // In a static field this is normally done only once
        try {
            eventCounter.register();
        } catch (Exception e) {
            // Ignore if already registered
        }
    }
    @Override
    public void invoke(EventCount value, Context context) throws IOException {
        try {
            // Update the Gauge with the aggregated value and set the event_type as a label
            eventCounter.labels(value.eventType).set(value.eventCount);

            // Push the metrics to the Pushgateway
            pushGateway.pushAdd(eventCounter, jobName);

        } catch (Exception e) {
            // Handle exceptions appropriately, e.g., logging them
            System.err.println("Failed to push metrics to Pushgateway: " + e.getMessage());
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        // Optional: Perform any cleanup here if necessary
    }
}
