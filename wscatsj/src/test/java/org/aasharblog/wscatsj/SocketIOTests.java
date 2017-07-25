package org.aasharblog.wscatsj;

import static org.junit.Assert.*;

import org.aasharblog.wscatsj.MarketDataFeed;
import org.aasharblog.wscatsj.SocketIOClient;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SocketIOTests {
	private static final Logger log = LoggerFactory.getLogger(SocketIOTests.class);
	final static String DEF_URI = "https://ws-api.iextrading.com/1.0/tops";
	final static int QUEUE_SIZE = 1000;
	final static int JMQ_PORT = 8076;
	final static String INIT_SYMS = "amzn,nflx,googl,orcl,fb,aig+";
	final static long WAIT_TIME = 2000;
	final static short POLL_TEST_LOOP = 5;

	static MarketDataFeed feed;
	static Thread feedHandlerThread;
	static ZContext zCtx = new ZContext(1);
	static Socket zSocket = zCtx.createSocket(ZMQ.REQ);

	@BeforeClass
	public static void setUp() throws Exception {
		feed = new SocketIOClient(DEF_URI, JMQ_PORT, QUEUE_SIZE, INIT_SYMS);

		feedHandlerThread = new Thread(new Runnable() {
	        public void run() {
	        	feed.connect();
	        }
	    });
	    feedHandlerThread.start();
        zSocket.connect("tcp://localhost:" + JMQ_PORT);
		Thread.sleep(WAIT_TIME);  // Wait for the socket connection
	}

	/**
	 * Test method for {@link org.aasharblog.wscatsj.SocketIOClient#feedActiveStatus()}.
	 */
	@Test
	public void t01_testFeedActiveStatus() {
		if (feed.feedActiveStatus()) {
			log.info("Command interface connected status test!");

	        zSocket.send("{\"cmd\":\"status\"}".getBytes(ZMQ.CHARSET), 0);
	        String status = new String(zSocket.recv(0), ZMQ.CHARSET);
	        assertEquals("Connection status for a connected session should be \"connected\": ", status, "connected");
		} else {
			log.info("Command interface disconnected status test!");

			zSocket.send("{\"cmd\":\"status\"}".getBytes(ZMQ.CHARSET), 0);
	        String status = new String(zSocket.recv(0), ZMQ.CHARSET);
	        assertEquals("Connection status for a disconnected session should be \"disconnected\": ", status, "connected");
		}
	}

	/**
	 * Test method for {@link org.aasharblog.wscatsj.SocketIOClient#poll()}.
	 * @throws InterruptedException 
	 */
	@Test
	public void t02_testPoll() throws InterruptedException {
		log.info("Socket polling test!");

	    for (short i=0; i < POLL_TEST_LOOP && !feedHandlerThread.isInterrupted(); i++) {
			Thread.sleep(WAIT_TIME);

			if (feed.feedActiveStatus()) {
				log.info("Connected!");

				feed.poll()
					.stream()
				    	.forEach(msg -> {
				    		JSONObject obj = null;
				    		String key = "";
				    		try {
								obj = new JSONObject(msg);
								key = obj.getString("symbol");
							} catch (JSONException e) { }
							
				    		log.info("key: " + key + ", value: " + obj.toString());
				    	});
			}
			else {
				log.warn("not connected!");
			}
	    }
	}

	/**
	 * Test method for {@link org.aasharblog.wscatsj.SocketIOClient#subscribe(java.lang.String[])}.
	 */
	@Test
	public void t03_testSubscribe() {
		if (feed.feedActiveStatus()) {
			log.info("Command interface subscribe test!");
	
	        zSocket.send("{\"cmd\":\"subscribe\",\"symbols\":\"ibm,aapl\"}".getBytes(ZMQ.CHARSET), 0);
	        String status = new String(zSocket.recv(0), ZMQ.CHARSET);
	        assertEquals("Expected response for a subscribe action should be \"accepted\": ", status, "accepted");
		}
	}

	/**
	 * Test method for {@link org.aasharblog.wscatsj.SocketIOClient#unsubscribe(java.lang.String[])}.
	 */
	@Test
	public void t04_testUnsubscribe() {
		if (feed.feedActiveStatus()) {
			log.info("Command interface unsubscribe test!");
	
	        zSocket.send("{\"cmd\":\"unsubscribe\",\"symbols\":\"ibm,aapl\"}".getBytes(ZMQ.CHARSET), 0);
	        String status = new String(zSocket.recv(0), ZMQ.CHARSET);
	        assertEquals("Expected response for an unsubscribe action should be \"accepted\": ", status, "accepted");
		}
	}

	@AfterClass
	public static void tearDown() throws Exception {
        zSocket.close();
        zCtx.close();

		feed.terminate();
	    feedHandlerThread.join();
	}
}
