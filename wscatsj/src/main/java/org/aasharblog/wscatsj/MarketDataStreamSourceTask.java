package org.aasharblog.wscatsj;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MarketDataStreamSourceTask extends SourceTask {
	private static final Logger log = LoggerFactory.getLogger(MarketDataStreamSourceTask.class);
	Map<String, String> sourcePartition;

	MarketDataFeed feed;
	private String kTopic;

	@Override
	public String version() {
        return new MarketDataStreamSourceConnector().version();
	}

	@Override
	public void start(Map<String, String> props) {
		String wsUri = props.get(MarketDataStreamSourceConnector.URI_CONFIG);
		String subSyms = props.get(MarketDataStreamSourceConnector.SUB_SYMS_CONFIG);
		kTopic = props.get(MarketDataStreamSourceConnector.TOPIC_CONFIG);
		String queueSizeString = props.get(MarketDataStreamSourceConnector.QUEUE_SIZE_CONFIG);
		String zmqPort = props.get(MarketDataStreamSourceConnector.ZMQ_PORT_CONFIG);
		log.trace("starting market data connect source for topic " + kTopic);
 
		try {
			feed = new SocketIOClient(wsUri, Integer.parseInt(zmqPort), Integer.parseInt(queueSizeString), subSyms);
			feed.connect();
			while (!feed.feedActiveStatus()) {
				log.trace("waiting for socket connection");
				Thread.sleep(1000);
			}
		} catch (Exception e) {
			feed = null;
            throw new ConnectException("Error in creating socket", e);
		}
		sourcePartition = Collections.singletonMap(MarketDataStreamSourceConnector.URI_CONFIG, wsUri);
		log.info("started market data connect source for topic " + kTopic);
	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		Map<String, Object> sourceOffset = Collections.singletonMap("position", null);
	    ArrayList<SourceRecord> records = new ArrayList<>();

	    feed.poll()
	    	.stream()
	    	.forEach(msg -> {
	    		String key = "";
	    		try {
					JSONObject obj = new JSONObject(msg);
					key = obj.getString("symbol");
				} catch (JSONException e) {
					// Ignoring non-JSON messages
				}

	    		records.add(new SourceRecord(sourcePartition, sourceOffset,
	    				kTopic, Schema.STRING_SCHEMA, key, Schema.STRING_SCHEMA, msg));
	    	});

		log.trace("records: " + Integer.toString(records.size())
				+ ", sc active: " + Boolean.toString(feed.feedActiveStatus()));

		return records;
	}

	@Override
	public void stop() {
		log.trace("stopping...");
		try {
			feed.terminate();
			log.debug("stopped...");
		} catch (InterruptedException e) {
			log.error("Error while stopping", e);
		}
	}
}
