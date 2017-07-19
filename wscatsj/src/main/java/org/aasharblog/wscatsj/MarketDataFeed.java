package org.aasharblog.wscatsj;

import org.zeromq.ZMQ;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;

import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MarketDataFeed {
	private static final Logger log = LoggerFactory.getLogger(MarketDataFeed.class);
	static final int DEF_JMQ_PORT = 8076;
	int jmqPort;

	CommandHandler cmdHandler = new CommandHandler();
	
	public abstract void start();
	public abstract void subscribe(String... symbols);
	public abstract void unsubscribe(String... symbols);
	public abstract boolean feedActiveStatus();
	public abstract List<String> poll();
	
	public MarketDataFeed() {
		this(DEF_JMQ_PORT);
	}
	
	public MarketDataFeed(int jmqPort) {
		this.jmqPort = jmqPort;
		cmdHandler.start();
	}

	public void stop() throws InterruptedException {
		cmdHandler.interrupt();
		cmdHandler.join();
	}

	public class CommandHandler extends Thread {
		final ZContext zCtx = new ZContext(1);
		final Socket cmdListener = zCtx.createSocket(ZMQ.REP);

		public CommandHandler() {
			this.setName("Command Handler");
		}
		
		public void run() {
			log.info("Command Handler starting on port " + Integer.toString(jmqPort));
			cmdListener.bind("tcp://*:" + Integer.toString(jmqPort));

			cmdListener.setReceiveTimeOut(1000);
			while (!Thread.currentThread().isInterrupted()) {
				String msg = "";

				byte[] msgBytes = null;

				try {
					msgBytes = cmdListener.recv(0);
				}
				catch (Exception e) {
					log.error("Socket recv error", e);
                    break;
				}

				if (msgBytes == null || msgBytes.length == 0)
					continue;
				else
					msg = new String(msgBytes, ZMQ.CHARSET);
				
				try {
					JSONObject obj = new JSONObject(msg);
					String cmd = obj.getString("cmd");
					if (cmd.equals("subscribe")) {
						String symbols = obj.getString("symbols");
						subscribe(symbols.split(","));
						cmdListener.send("success");
					} else if (cmd.equals("unsubscribe")) {
						String symbols = obj.getString("symbols");
						unsubscribe(symbols.split(","));
						cmdListener.send("success");
					} else { 
						log.info("Invalid command:" + msg);
						cmdListener.send("Invalid command:" + msg);
					}
				} catch (JSONException e) {
					cmdListener.send("unsuccess:" + e.getMessage());
				}
			}

			log.debug("Closing zmqsocket");
			cmdListener.setLinger(0);
			try { cmdListener.close(); } // This may throw an error for poor thread interrupt handler in jmq
			catch (Exception ex) { }
			log.info("closed zmqsocket");

			log.debug("destroying ctx");
			try { zCtx.destroy(); } // This may throw an error for poor thread interrupt handler in jmq
			catch (Exception ex) { }
			log.info("destroyed ctx");
		}
	}
}
