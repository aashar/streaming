package org.aasharsite.zmq_cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Application {
	private static final Logger log = LoggerFactory.getLogger(Application.class);
	static final int jmqPort = 8076; // TODO: Configurable
	final static ZContext zCtx = new ZContext(1);
	final static Socket socket = zCtx.createSocket(ZMQ.REQ);
	
	public static void main(String[] args) throws IOException {
		socket.connect("tcp://localhost:8076");

        BufferedReader br = null;

        System.out.println("Type commands or");
        System.out.println("help for sample commands or");
        System.out.println("exit to quit:");
		while (true) {
            br = new BufferedReader(new InputStreamReader(System.in));
            System.out.print("cmd$ ");
            String input = br.readLine();

            if ("EXIT".equals(input.toUpperCase())) {
                System.out.println("Exiting");
                System.exit(0);
            } else if ("HELP".equals(input.toUpperCase())) {
                System.out.println("{\"cmd\":\"subscribe\", \"symbols\":(\"aapl\",\"ibm\")}");
                System.out.println("{\"cmd\":\"unsubscribe\", \"symbols\":(\"aapl\",\"ibm\")}");
                continue;
            }

            log.debug("input: " + input);

            socket.send(input.getBytes(ZMQ.CHARSET), 0);
            byte[] reply = socket.recv(0);
            System.out.println("Received " + new String(reply, ZMQ.CHARSET));
        }
	}
}
