package communication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotEquals;

import java.util.List;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zeromq.ZMQ;

import crypto.CryptoProvider;
import crypto.CryptoProvider.EncryptionAlgorithm;
import model.JSONable;
import model.data.KeygroupID;
import model.messages.datarecords.Envelope;
import model.messages.datarecords.Message;

public class PublisherTest {

	private static Logger logger = Logger.getLogger(PublisherTest.class.getName());

	private Publisher publisher = null;
	public String secret = "testSecret";
	public EncryptionAlgorithm algorithm = EncryptionAlgorithm.AES;

	@Before
	public void setUp() throws Exception {
		publisher = new Publisher("tcp://localhost", 6204, secret, algorithm);
	}

	@After
	public void tearDown() throws Exception {
		publisher.shutdown();
		logger.debug("\n");
	}

	@Test
	public void testShutdown() {
		logger.debug("-------Starting testShutdown-------");
		publisher.shutdown();
		assertTrue(publisher.isShutdown());
		logger.debug("Finished testShutdown.");
	}

	@Test
	public void testPublishOne() throws InterruptedException {
		logger.debug("-------Starting testPublishOne-------");
		Message m = new Message();
		m.setContent("Test content");
		Envelope e = new Envelope(new KeygroupID("app", "tenant", "group"), m);
		Thread t = new Thread(new SubscribeHelper(e));
		t.start();
		Thread.sleep(400);
		publisher.send(e);
		t.join();
		logger.debug("Finished testPublishOne.");
	}

	@Test
	public void testPublishTwo() throws InterruptedException {
		logger.debug("-------Starting testPublishTwo-------");
		Message m = new Message();
		m.setContent("Test content");
		Envelope e = new Envelope(new KeygroupID("app", "tenant", "group"), m);
		Thread t1 = new Thread(new SubscribeHelper(e));
		Thread t2 = new Thread(new SubscribeHelper(e));
		t1.start();
		t2.start();
		Thread.sleep(400);
		publisher.send(e);
		t1.join();
		t2.join();
		logger.debug("Finished testPublishTwo.");
	}
	
	@Test
	public void testPublishMany() throws InterruptedException {
		logger.debug("-------Starting testPublishMany-------");
		Message m = new Message();
		m.setContent("Test content");
		Envelope e = new Envelope(new KeygroupID("app", "tenant", "group"), m);
		List<Thread> list = new ArrayList<Thread>();
		for (int i = 0; i < 100; i++) {
			Thread t = new Thread(new SubscribeHelper(e));
			list.add(t);
			t.start();
		}
		Thread.sleep(400);
		for (int i = 0; i < 100; i++) {
			publisher.send(e);
		}
		for (int i = 0; i < 100; i++) {
			list.get(i).join();
		}
		logger.debug("Finished testPublishMany.");
	}

	class SubscribeHelper implements Runnable {

		private Envelope e = null;
		
		public SubscribeHelper(Envelope	e) {
			this.e = e;
		}
		
		@Override
		public void run() {
			ZMQ.Context context = ZMQ.context(1);
			ZMQ.Socket subscriber = context.socket(ZMQ.SUB);
			subscriber.connect(publisher.getAddress() + ":" + publisher.getPort());
			subscriber.subscribe(e.getKeygroupID().toString().getBytes());
			String namespace = subscriber.recvStr(ZMQ.SNDMORE);
			String received = subscriber.recvStr();
			assertNotEquals(received, e.getMessage().getContent());
			String content = CryptoProvider.decrypt(received, secret, algorithm);
			subscriber.close();
			context.term();
			assertEquals(namespace, e.getKeygroupID().toString());
			assertEquals(content, JSONable.toJSON(e.getMessage()));
		}
		
	}
	
}
