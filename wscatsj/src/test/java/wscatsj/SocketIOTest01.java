package wscatsj;

import static org.junit.Assert.*;

import java.net.URISyntaxException;

import org.aasharblog.wscatsj.MarketDataFeed;
import org.aasharblog.wscatsj.SocketIOClient;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

public class SocketIOTest01 {
	final String DEF_URI = "https://ws-api.iextrading.com/1.0/tops";
	final int QUEUE_SIZE = 1000;
	final String INIT_SYMS = "orcl,fb,aig+";
	final String INIT_UNSYMS = "aig+";
	final long WAIT_TIME = 2000;
	final String SOCKET_THREAD_NAME = "IEXThread";
	final short TEST_LOOP = 5;

	@Test
	public void test() {
		try {
			MarketDataFeed sc
				= new SocketIOClient(DEF_URI, Integer.toString(QUEUE_SIZE), INIT_SYMS, INIT_UNSYMS);

			Thread t = new Thread(new Runnable() {
		        public void run() {
		        	sc.start();
		        }
		    });
			t.setName(SOCKET_THREAD_NAME);
		    t.start();

		    for (short i=0; i < TEST_LOOP; i++) {
			    Thread.sleep(WAIT_TIME);

				if (sc.feedActiveStatus()) {
					System.out.println("still going!");
				}
				else
					System.out.println("not connected!");

				sc.poll()
					.stream()
				    	.forEach(msg -> {
				    		String key = "";
				    		try {
								JSONObject obj = new JSONObject(msg);
								key = obj.getString("symbol");
							} catch (JSONException e) { }
							
							System.out.println("key: " + key + ", value: " + msg);
				    	});
		    }

			sc.stop();
		    t.join();
		} catch (URISyntaxException e) {
			fail("URI syntax error:" + e.getMessage());
		} catch (Exception e) {
			fail("Error in engine:" + e.getMessage());
		}
	}
}
