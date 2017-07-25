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
	private static final int DEF_JMQ_PORT = 8076;

	CommandHandler cmdHandler;
	
	public abstract void subscribe(String... symbols);
	public abstract void unsubscribe(String... symbols);
	public abstract void connect();
	public abstract void disconnect();
	public abstract boolean feedActiveStatus();
	public abstract List<String> poll();
	
	public MarketDataFeed() {
		this(DEF_JMQ_PORT);
	}

	public MarketDataFeed(int jmqPort) {
		cmdHandler = new CommandHandler(jmqPort);
		cmdHandler.start();
	}

	public void reconnect() {
		disconnect();
		connect();
	}

	public void terminate() throws InterruptedException {
		cmdHandler.interrupt();
		cmdHandler.join();
	}

	public class CommandHandler extends Thread {
		int jmqPort;
		final ZContext zCtx = new ZContext(1);
		final Socket cmdListener = zCtx.createSocket(ZMQ.REP);

		public CommandHandler(int jmqPort) {
			this.jmqPort = jmqPort;
			this.setName("Command Handler");
		}
		
		public void run() {
			log.info("Command Handler starting on port " + Integer.toString(jmqPort));
			cmdListener.bind("tcp://*:" + Integer.toString(jmqPort));

			cmdListener.setReceiveTimeOut(1000);
			while (!Thread.currentThread().isInterrupted()) {
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
				else {
					String msg = new String(msgBytes, ZMQ.CHARSET);
					JSONObject obj = null;

					try {
						obj = new JSONObject(msg);
						cmdListener.send(processCommand(obj));
					} catch (JSONException e) {
						cmdListener.send("unsuccess:" + e.getMessage());
					}
				}
			}

			log.trace("Closing zmqsocket");
			cmdListener.setLinger(0);
			try { cmdListener.close(); } // This may throw an error for poor thread interrupt handler in jmq
			catch (Exception ex) { }
			log.info("closed zmqsocket");

			log.trace("destroying ctx");
			try { zCtx.destroy(); } // This may throw an error for poor thread interrupt handler in jmq
			catch (Exception ex) { }
			log.info("destroyed ctx");
		}
		
		String processCommand(JSONObject obj) throws JSONException {
			String retString = "";
			String symbols;

			switch (obj.getString("cmd")) {
				case "subscribe" :
					symbols = obj.getString("symbols");
					subscribe(symbols.split(","));
					retString ="accepted";
					break;
				case "unsubscribe" :
					symbols = obj.getString("symbols");
					unsubscribe(symbols.split(","));
					retString ="accepted";
					break;
				case "status" :
					retString = feedActiveStatus() ? "connected" : "disconnected";
					break;
				case "connect" :
					if (feedActiveStatus())
						retString ="already connected";
					else {
						connect();
						retString ="accepted";
					}
					break;
				case "disconnect" :
					if (!feedActiveStatus())
						retString ="already disconnected";
					else {
						disconnect();
						retString ="accepted";
					}
					break;
				case "reconnect" :
					if (feedActiveStatus()) {
						reconnect();
						retString ="accepted";
					} else {
						connect();
						retString ="accepted";
					}
					break;
				default :
					log.info("Invalid command:" + obj.toString());
					retString ="Invalid command:" + obj.toString();
			}

			return retString;
		}
	}
}
