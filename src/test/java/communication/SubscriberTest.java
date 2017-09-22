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
import exceptions.FBaseEncryptionException;
import model.JSONable;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.messages.Message;
import tasks.FBaseFactory;

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
	public static FBase fBase = null;

	static ZMQ.Context contextPub = null;
	static ZMQ.Socket publisher = null;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		executor = Executors.newCachedThreadPool();
		contextPub = ZMQ.context(1);
		publisher = contextPub.socket(ZMQ.PUB);
		publisher.bind(address + ":" + port);
		fBase = FBaseFactory.basic(1, false, false);
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
		fBase.tearDown();
	}

	@Test
	public void testSubscribe() throws InterruptedException, ExecutionException, TimeoutException,
			FBaseEncryptionException {
		logger.debug("-------Starting testSubscribe-------");
		Message m = new Message();
		Subscriber subscriber = new Subscriber(address, port, secret, algorithm, fBase);
		subscriber.startReceiving();
		m.setContent(JSONable.toJSON(update));
		m.encryptFields(secret, algorithm);
		publisher.sendMore(update.getDataIdentifier().getKeygroupID().getID());
		publisher.send(JSONable.toJSON(m));
		Thread.sleep(500);
		m.decryptFields(secret, algorithm);
		assertEquals(1, subscriber.getNumberOfReceivedMessages());
		m.setContent(JSONable.toJSON(update2));
		m.encryptFields(secret, algorithm);
		logger.debug("Sending second message");
		publisher.sendMore(update2.getDataIdentifier().getKeygroupID().getID());
		publisher.send(JSONable.toJSON(m));
		Thread.sleep(500);
		assertEquals(2, subscriber.getNumberOfReceivedMessages());
		logger.debug("Finished testSubscribe.");
	}

	@Test
	public void testSubscribeWithFilter() throws InterruptedException, ExecutionException,
			TimeoutException, FBaseEncryptionException {
		logger.debug("-------Starting testSubscribeWithFilter-------");
		Message m = new Message();
		Subscriber subscriber =
				new Subscriber(address, port, secret, algorithm, fBase, update.getKeygroupID());
		subscriber.startReceiving();
		m.setContent(JSONable.toJSON(update));
		m.encryptFields(secret, algorithm);
		publisher.sendMore(update.getDataIdentifier().getKeygroupID().getID());
		publisher.send(JSONable.toJSON(m));
		Thread.sleep(500);
		assertEquals(1, subscriber.getNumberOfReceivedMessages());
		m.setContent(JSONable.toJSON(update2));
		publisher.sendMore(update2.getDataIdentifier().getKeygroupID().getID());
		publisher.send(CryptoProvider.encrypt(JSONable.toJSON(m), secret, algorithm));
		Thread.sleep(500);
		assertEquals(1, subscriber.getNumberOfReceivedMessages());
		logger.debug("Finished testSubscribeWithFilter.");
	}

}
