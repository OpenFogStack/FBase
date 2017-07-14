package communication;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zeromq.ZMQ;

import control.FBase;
import crypto.CryptoProvider;
import crypto.CryptoProvider.EncryptionAlgorithm;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.message.keygroup.KeygroupMessage;

public class SubscriberTest {

	private static Logger logger = Logger.getLogger(SubscriberTest.class.getName());
	
	private static ExecutorService executor;
	public String secret = "testSecret";
	public EncryptionAlgorithm algorithm = EncryptionAlgorithm.AES;
	DataRecord update = null;
	DataRecord update2 = null;
	public static String address = "tcp://localhost";
	public static int port = 6701;
	Subscriber subscriber = null;
	
	static ZMQ.Context contextPub = null;
	static ZMQ.Socket publisher = null;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		executor = Executors.newCachedThreadPool();
		contextPub = ZMQ.context(1);
		publisher = contextPub.socket(ZMQ.PUB);
		publisher.bind(address + ":" + port);
		@SuppressWarnings("unused")
		FBase fbase = new FBase(null);
	}
	
	@Before
	public void setUp() throws Exception {
		update = new DataRecord();
		update.setDataIdentifier(new DataIdentifier("a", "b", "c", "1"));
		update.setValueWithoutKey("Test Value Update abc1");
		update2 = new DataRecord();
		update2.setDataIdentifier(new DataIdentifier("a", "bb", "a", "1"));
		update2.setValueWithoutKey("Test Value Update abba1");
	}

	@After
	public void tearDown() throws Exception {
		if (subscriber != null) {
			subscriber.stopReception();
			subscriber = null;
		}
		logger.debug("\n");
	}
	
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		executor.shutdownNow();
		publisher.close();
	    contextPub.term();
	}

	@Test
	public void testSubscribe() throws InterruptedException, ExecutionException, TimeoutException {
		logger.debug("-------Starting testSubscribe-------");
		KeygroupMessage m = new KeygroupMessage();
		Subscriber subscriber = new Subscriber(address, port, secret, algorithm);
		subscriber.startReception();
		m.setContent(update.toJSON());
		publisher.sendMore(update.getDataIdentifier().getKeygroupID().toString());
	    publisher.send(CryptoProvider.encrypt(m.toJSON(), secret, algorithm));
		Thread.sleep(500);
		assertEquals(1, subscriber.getNumberOfReceivedMessages());
		m.setContent(update2.toJSON());
		publisher.sendMore(update2.getDataIdentifier().getKeygroupID().toString());
	    publisher.send(CryptoProvider.encrypt(m.toJSON(), secret, algorithm));
		Thread.sleep(500);
		assertEquals(2, subscriber.getNumberOfReceivedMessages());
		logger.debug("Finished testSubscribe.");
	}
	
	@Test
	public void testSubscribeWithFilter() throws InterruptedException, ExecutionException, 
	TimeoutException {
		logger.debug("-------Starting testSubscribeWithFilter-------");
		KeygroupMessage m = new KeygroupMessage();
		Subscriber subscriber = new Subscriber(address, port, secret, algorithm, update.getKeygroupID());
		subscriber.startReception();
		m.setContent(update.toJSON());
		publisher.sendMore(update.getDataIdentifier().getKeygroupID().toString());
	    publisher.send(CryptoProvider.encrypt(m.toJSON(), secret, algorithm));
		Thread.sleep(500);
		assertEquals(1, subscriber.getNumberOfReceivedMessages());
		m.setContent(update2.toJSON());
		publisher.sendMore(update2.getDataIdentifier().getKeygroupID().toString());
	    publisher.send(CryptoProvider.encrypt(m.toJSON(), secret, algorithm));
		Thread.sleep(500);
		assertEquals(1, subscriber.getNumberOfReceivedMessages());
		logger.debug("Finished testSubscribeWithFilter.");
	}

}
