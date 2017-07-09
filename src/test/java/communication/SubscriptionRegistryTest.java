package communication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Test;

import crypto.CryptoProvider.EncryptionAlgorithm;
import model.data.DataRecord;
import model.data.KeygroupID;
import model.message.Envelope;
import model.message.Message;

public class SubscriptionRegistryTest {

	private static Logger logger = Logger.getLogger(SubscriptionRegistryTest.class.getName());
		
	@After
	public void tearDown() throws Exception {
		SubscriptionRegistry.deleteAllData();
		logger.debug("\n");
	}
	
	@Test
	public void testSubscribe() throws InterruptedException {
		logger.debug("-------Starting testSubscribe-------");
		Subscriber s = SubscriptionRegistry.subscribeTo("tcp://localhost", 8081, "secret", EncryptionAlgorithm.AES, null);
		assertEquals(1, SubscriptionRegistry.getNumberOfActiveSubscriptions());
		Publisher publisher = new Publisher("tcp://localhost", 8081, "secret", EncryptionAlgorithm.AES);
		Thread.sleep(200);
		Message m = new Message();
		DataRecord record = new DataRecord();
		record.setValueWithoutKey("Test Value");
		m.setContent(record.toJSON());
		Envelope e = new Envelope(new KeygroupID("app", "tenant", "group"), m);
		publisher.send(e);
		Thread.sleep(200);
		assertEquals(1, s.getNumberOfReceivedMessages());
		logger.debug("Finished testSubscribe.");
	}
	
	@Test
	public void testUnSubscribe() {
		logger.debug("-------Starting testUnSubscribe-------");
		SubscriptionRegistry.subscribeTo("tcp://localhost", 8081, "secret", EncryptionAlgorithm.AES, null);
		assertEquals(1, SubscriptionRegistry.getNumberOfActiveSubscriptions());
		SubscriptionRegistry.unsubscribeFrom("tcp://localhost", 8081);
		assertEquals(0, SubscriptionRegistry.getNumberOfActiveSubscriptions());
		logger.debug("Finished testUnSubscribe.");
	}
	
	@Test
	public void testSubscribeTwoSameAddressUnsubscribe() {
		logger.debug("-------Starting testSubscribeTwoSameAddressUnsubscribe-------");
		SubscriptionRegistry.subscribeTo("tcp://localhost", 8081, "secret", EncryptionAlgorithm.AES, null);
		SubscriptionRegistry.subscribeTo("tcp://localhost", 8082, "secret", EncryptionAlgorithm.AES, null);
		assertEquals(2, SubscriptionRegistry.getNumberOfActiveSubscriptions());
		SubscriptionRegistry.unsubscribeFrom("tcp://localhost", 8081);
		assertEquals(1, SubscriptionRegistry.getNumberOfActiveSubscriptions());
		assertEquals(1, SubscriptionRegistry.getNumberOfActiveSubscriptions("tcp://localhost"));
		logger.debug("Finished testSubscribeTwoSameAddressUnsubscribe.");
	}
	
	@Test
	public void testSubscribeSamePortOneAddress() {
		logger.debug("-------Starting testSubscribeSamePortOneAddress-------");
		SubscriptionRegistry.subscribeTo("tcp://localhost", 8081, "secret", EncryptionAlgorithm.AES, null);
		assertEquals(1, SubscriptionRegistry.getNumberOfActiveSubscriptions());
		assertNull(SubscriptionRegistry.subscribeTo("tcp://localhost", 8081, "secret", EncryptionAlgorithm.AES, null));
		assertEquals(1, SubscriptionRegistry.getNumberOfActiveSubscriptions());
		logger.debug("Finished testSubscribeSamePortOneAddress.");
	}
	
	@Test
	public void testSubscribeSamePortDifferentAddress() {
		logger.debug("-------Starting testSubscribeSamePortDifferentAddress-------");
		SubscriptionRegistry.subscribeTo("tcp://10.0.1.1", 8081, "secret", EncryptionAlgorithm.AES, null);
		assertEquals(1, SubscriptionRegistry.getNumberOfActiveSubscriptions());
		SubscriptionRegistry.subscribeTo("tcp://10.0.1.2", 8081, "secret", EncryptionAlgorithm.AES, null);
		assertEquals(2, SubscriptionRegistry.getNumberOfActiveSubscriptions());
		assertEquals(1, SubscriptionRegistry.getNumberOfActiveSubscriptions("tcp://10.0.1.1"));
		assertEquals(1, SubscriptionRegistry.getNumberOfActiveSubscriptions("tcp://10.0.1.2"));
		logger.debug("Finished testSubscribeSamePortDifferentAddress.");
	}
	
	@Test
	public void failUnsubscribe() {
		logger.debug("-------Starting failUnsubscribe-------");
		assertFalse(SubscriptionRegistry.unsubscribeFrom("localhost", 8081));
		logger.debug("Finished failUnsubscribe.");
	}
	
}
