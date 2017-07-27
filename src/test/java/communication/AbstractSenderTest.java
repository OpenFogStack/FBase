package communication;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zeromq.ZMQ;

import crypto.CryptoProvider.EncryptionAlgorithm;
import model.data.KeygroupID;
import model.messages.datarecords.Envelope;
import model.messages.datarecords.Message;

public class AbstractSenderTest {

	private static Logger logger = Logger.getLogger(AbstractSenderTest.class.getName());
	private static ExecutorService executor;
	private Sender sender = null;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		executor = Executors.newCachedThreadPool();
	}

	@Before
	public void setUp() throws Exception {
		sender = new Sender("tcp://localhost", 6201, null, null, ZMQ.REQ);
	}

	@After
	public void tearDown() throws Exception {
		sender.shutdown();
		logger.debug("\n");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		executor.shutdownNow();
	}

	@Test
	public void test() throws InterruptedException, ExecutionException, TimeoutException {
		logger.debug("-------Starting test-------");
		Message m = new Message();
		m.setContent("Test content");
		Envelope e = new Envelope(new KeygroupID("app", "tenant", "group"), m);
		Future<?> future = executor.submit(new ReceiveHelper(e));
		String response = sender.send(e);
		future.get(5, TimeUnit.SECONDS);
		assertEquals("Success", response);
		logger.debug("Finished test.");
	}

	class Sender extends AbstractSender {

		public Sender(String address, int port, String secret, EncryptionAlgorithm algorithm,
				int senderType) {
			super(address, port, secret, algorithm, senderType);
		}

		@Override
		public String send(Envelope envelope) {
			logger.debug("Sending " + envelope.toString());
			sender.sendMore(envelope.getKeygroupID().toString());
			sender.send(envelope.getMessage().getContent());
			return sender.recvStr();
		}

	}

	class ReceiveHelper implements Runnable {

		private Envelope envelope = null;

		public ReceiveHelper(Envelope envelope) {
			this.envelope = envelope;
		}

		@Override
		public void run() {
			ZMQ.Context context = ZMQ.context(1);
			ZMQ.Socket receiver = context.socket(ZMQ.REP);
			receiver.bind(sender.getAddress() + ":" + sender.getPort());
			KeygroupID keygroupID = KeygroupID.createFromString(receiver.recvStr());
			Message m = new Message();
			m.setContent(receiver.recvStr());
			logger.info("Received " + new Envelope(keygroupID, m).toString());
			receiver.send("Success");
			receiver.close();
			context.term();
			assertEquals(envelope.getKeygroupID(), keygroupID);
			assertEquals(envelope.getMessage(), m);
		}

	}

}
