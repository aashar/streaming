package org.aasharblog.wscatsj;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

public class MarketDataStreamSourceConnector extends SourceConnector {
	public static final String URI_CONFIG = "uri";
	public static final String SUB_SYMS_CONFIG = "subscribe_symbols";
	public static final String TOPIC_CONFIG = "topic";
	public static final String QUEUE_SIZE_CONFIG = "queue_size";
	public static final String ZMQ_PORT_CONFIG = "zmq_port";

	private static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(URI_CONFIG, Type.STRING, null, Importance.HIGH, "Socket URI")
        .define(QUEUE_SIZE_CONFIG, Type.STRING, null, Importance.HIGH, "Queue Size (int)")
        .define(SUB_SYMS_CONFIG, Type.STRING, Importance.HIGH, "List of symbols")
        .define(TOPIC_CONFIG, Type.STRING, Importance.HIGH, "Topic name")
		.define(ZMQ_PORT_CONFIG, Type.STRING, Importance.HIGH, "ZMQ Port for administration");

	private String wsUri;
	private String subSyms;
	private String topic;
	private String queueSizeString;
	private String zmqPort;

	@Override
	public String version() {
        return AppInfoParser.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		wsUri = props.get(URI_CONFIG);
		subSyms = props.get(SUB_SYMS_CONFIG);
		queueSizeString = props.get(QUEUE_SIZE_CONFIG);
		topic = props.get(TOPIC_CONFIG);
		zmqPort = props.get(ZMQ_PORT_CONFIG);

		if (wsUri == null || wsUri.isEmpty())
			wsUri = "https://ws-api.iextrading.com/1.0/tops";
		if (queueSizeString == null || queueSizeString.isEmpty())
			queueSizeString = "1000";
		if (subSyms == null || subSyms.isEmpty())
			subSyms = "snap,fb,aig+";
		if (topic == null || topic.isEmpty())
			topic = "mktdata";
		if (zmqPort == null || zmqPort.isEmpty())
			zmqPort = "mktdata";
	}

	@Override
	public Class<? extends Task> taskClass() {
		return MarketDataStreamSourceTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		ArrayList<Map<String, String>> configs = new ArrayList<Map<String, String>>();
        Map<String, String> config = new HashMap<>();
        config.put(URI_CONFIG, wsUri);
        config.put(SUB_SYMS_CONFIG, subSyms);
        config.put(TOPIC_CONFIG, topic);
		config.put(QUEUE_SIZE_CONFIG, queueSizeString);
		config.put(ZMQ_PORT_CONFIG, zmqPort);
        configs.add(config);
        return configs;
    }

	@Override
	public void stop() {
	}

	@Override
	public ConfigDef config() {
		return CONFIG_DEF;
	}
}
