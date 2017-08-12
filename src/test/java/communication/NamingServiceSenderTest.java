package communication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

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

import exceptions.FBaseNamingServiceException;
import model.JSONable;
import model.data.KeygroupID;
import model.messages.Envelope;
import model.messages.Message;

public class NamingServiceSenderTest {

	private static Logger logger = Logger.getLogger(NamingServiceSenderTest.class.getName());

	private static ExecutorService executor;
	NamingServiceSender sender = null;
	String address = "tcp://localhost";
	int port = 1234;
	Envelope e = null;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		executor = Executors.newCachedThreadPool();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		executor.shutdownNow();
	}

	@Before
	public void setUp() throws Exception {
		sender = new NamingServiceSender(address, port, null);
		Message m = new Message();
		m.setTextualInfo("TestText");
		e = new Envelope(new KeygroupID("a", "b", "c"), m);
	}

	@After
	public void tearDown() throws Exception {
		sender.shutdown();
		logger.debug("\n");
	}

	@Test
	public void testPollingSuccess() throws InterruptedException, ExecutionException,
			TimeoutException, FBaseNamingServiceException {
		logger.debug("-------Starting testPollingSuccess-------");
		Future<?> future = executor.submit(new ReceiveHelper());
		Thread.sleep(200);
		String reply = sender.send(e, null, null);
		future.get(5, TimeUnit.SECONDS);
		assertEquals("Success", reply);
		logger.debug("Finished testPollingSuccess.");
	}

	@Test
	public void testPollingNoResponse() {
		logger.debug("-------Starting testPollingNoResponse-------");
		try {
			@SuppressWarnings("unused")
			String reply = sender.send(e, null, null);
		} catch (FBaseNamingServiceException e) {
			logger.debug(e.getMessage());
			return;
		}
		fail("Should have catched an exception");
		logger.debug("Finished testPollingNoResponse.");
	}

	@Test
	public void testPollingRecovery() throws InterruptedException, FBaseNamingServiceException,
			ExecutionException, TimeoutException {
		logger.debug("-------Starting testPollingRecovery-------");
		try {
			@SuppressWarnings("unused")
			String reply = sender.send(e, null, null);
		} catch (FBaseNamingServiceException e) {
			logger.debug(e.getMessage());
		}
		Future<?> future = executor.submit(new ReceiveHelper());
		Thread.sleep(200);
		String reply = sender.send(e, null, null);
		future.get(5, TimeUnit.SECONDS);
		assertEquals("Success", reply);
		logger.debug("Finished testPollingRecovery.");
	}

	@Test
	public void testMalformattedURI() throws InterruptedException, FBaseNamingServiceException,
			ExecutionException, TimeoutException {
		logger.debug("-------Starting testMalformattedURI-------");
		NamingServiceSender malSender = new NamingServiceSender("asdlkfasdfjk", -1, null);
		String reply = malSender.send(e, null, null);
		assertNull(reply);
		logger.debug("Finished testMalformattedURI.");
	}

	class ReceiveHelper implements Runnable {

		@Override
		public void run() {
			ZMQ.Context context = ZMQ.context(1);
			ZMQ.Socket receiver = context.socket(ZMQ.REP);
			receiver.bind(sender.getAddress() + ":" + sender.getPort());
			String keygroupID = receiver.recvStr();
			logger.debug("Keygroup: " + keygroupID);
			String content = receiver.recvStr();
			Message m = JSONable.fromJSON(content, Message.class);
			logger.info("Message: " + m.getTextualInfo());
			receiver.send("Success");
			logger.debug("Closing receiver");
			receiver.close();
			context.term();
		}

	}

}
