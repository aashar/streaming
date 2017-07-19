package org.aasharblog.wscatsj;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.socket.client.IO;
import io.socket.client.Socket;
import io.socket.emitter.Emitter;

public class SocketIOClient extends MarketDataFeed {
	private static final Logger log = LoggerFactory.getLogger(SocketIOClient.class);

	final Socket socket;
	int queueSize = 1000;
	String[] subSyms = {"snap","amzn","googl", "fb","aig+"};

    private final BlockingQueue<String> queue;

	public SocketIOClient(String wsUri, int zmqPort, int queueSize, String subSyms) throws Exception {
		super(zmqPort);
		this.queueSize = queueSize;
		this.subSyms = subSyms.split(",");

		log.info("socket.io uri: " + wsUri);

		queue = new ArrayBlockingQueue<String>(queueSize);

		socket = IO.socket(wsUri);

		socket
		  .on(Socket.EVENT_CONNECT, new ConnectListener())
		  .on(Socket.EVENT_MESSAGE, new MessageListener());
	}

	@Override
	public void start() {
		socket.connect();
		log.debug("Socket connect command complete");
	}

	@Override
	public void stop() {
		socket.disconnect();
		try {
			super.stop();
			log.debug("Socket disconnect command complete");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			log.error("Failed to stop market data listener", e);
		}
	}

	@Override
	public void subscribe(String... symbols) {
		socket.emit("subscribe", String.join(",", symbols));
	}

	@Override
	public void unsubscribe(String... symbols) {
		socket.emit("unsubscribe", String.join(",", symbols));
	}

	@Override
	public boolean feedActiveStatus() {
		return socket.connected();
	}

	public class ConnectListener implements Emitter.Listener {
		@Override
		public void call(Object... args) {
			log.debug("Socket connected");
			subscribe(subSyms);
		}
	}

	public class MessageListener implements Emitter.Listener {
		@Override
		public void call(Object... args) {
			Arrays
				.asList(args)
				.stream()
				.forEachOrdered(quote -> queue.offer((String)quote));
		}
	}

	public List<String> poll() {
		log.debug("polling...");

		List<String> retList = new ArrayList<String>();
		if (queue.size() == 0) {
/*			retList.add("{\"symbol\":\"SNAP\",\"marketPercent\":0.00901,\"bidSize\":200," +
				"\"bidPrice\":110.94,\"askSize\":100,\"askPrice\":111.82,\"volume\":177265," +
				"\"lastSalePrice\":111.76,\"lastSaleSize\":5,\"lastSaleTime\":1480446905681," +
				"\"lastUpdated\":1480446910557}"); // for testing
*/			try {
				log.debug("No messages, waiting for a second");
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
		}
		else
			queue.drainTo(retList);
		
		return retList;
	}
}
