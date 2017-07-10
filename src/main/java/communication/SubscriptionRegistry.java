package communication;

import java.util.HashMap;

import org.apache.log4j.Logger;

import crypto.CryptoProvider.EncryptionAlgorithm;
import model.data.KeygroupID;

public class SubscriptionRegistry {

	private static Logger logger = Logger.getLogger(SubscriptionRegistry.class.getName());

	private volatile HashMap<String, HashMap<Integer, Subscriber>> activeSubscriptions = null;

	public SubscriptionRegistry() {
		activeSubscriptions = new HashMap<String, HashMap<Integer, Subscriber>>();
	}
	
	/**
	 * Starts a new Subscriber.
	 * 
	 * @param address
	 *            - the address to subscribe to
	 * @param port
	 *            - the port to subscribe to
	 * @param secret
	 *            - the secret used to decrypt received data
	 * @param algorithm
	 *            - the algorithm used for decryption
	 * @return the subscriber or null
	 */
	public synchronized Subscriber subscribeTo(String address, int port, String secret,
			EncryptionAlgorithm algorithm, KeygroupID keygroupIDFilter) {
		// Case 1: address port combination exists -> false
		if (subscriptionExists(address, port)) {
			logger.warn("Already subscribed to " + address + ":" + port);
			return null;
		}

		Subscriber subscriber = null;
		try {
			subscriber = new Subscriber(address, port, secret, algorithm, keygroupIDFilter);
			if (subscriber.startReception() == null) {
				throw new Exception("Could not start receiving.");
			}
		} catch (Exception e) {
			logger.error("Could not initialize Subscriber");
			e.printStackTrace();
		}
		
		// Case 2: subscriptions for address exist -> put port in already existent hashmap
		if (subscribedToAddress(address)) {
			activeSubscriptions.get(address).put(port, subscriber);
			return subscriber;
		}

		// Case 3: not subscribed to address yet -> create new hashmap
		HashMap<Integer, Subscriber> map = new HashMap<Integer, Subscriber>();
		map.put(port, subscriber);
		activeSubscriptions.put(address, map);
		return subscriber;
	}

	public synchronized boolean unsubscribeFrom(String address, int port) {
		// needs to delete port tuple
		// Case 1: was not subscribed -> false
		if (!subscriptionExists(address, port)) {
			return false;
		}
		
		HashMap<Integer, Subscriber> map = activeSubscriptions.get(address);
		Subscriber s = map.remove(port);
		s.stopReception();
		
		// Case 2: more subscriptions for address exist -> do nothing more
		if (!map.isEmpty()) {
			return true;
		}
		
		// Case 3: no more subscriptions for address exist -> remove key address
		activeSubscriptions.remove(address);
		return true;
	}

	/**
	 * Subscriptions to the given address exist, a value for the given key is present.
	 * 
	 * @param address
	 * @return true, if subscriptions exists
	 */
	public synchronized boolean subscribedToAddress(String address) {
		return activeSubscriptions.containsKey(address);
	}

	/**
	 * A subscription to the given address port combination exists, a value for this combination is present.
	 * 
	 * @param address
	 * @param port
	 * @return true, if subscriptions exists
	 */
	public synchronized boolean subscriptionExists(String address, int port) {
		if (subscribedToAddress(address)) {
			if (activeSubscriptions.get(address).containsKey(port)) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Returns the number of active subscriptions for a given address (over all ports).
	 * @param address
	 * @return the number or 0
	 */
	 public synchronized int getNumberOfActiveSubscriptions(String address) {
		 if (subscribedToAddress(address)) {
			 return activeSubscriptions.get(address).size();
		 }
		 return 0;
	 }
	 
	 /**
	  * Returns the number of all active subscriptions for all addresses and ports.
	  * @return the number or 0
	  */
	 public synchronized int getNumberOfActiveSubscriptions() {
		 if (activeSubscriptions.isEmpty()) {
			 return 0;
		 }
		 int number = 0;
		 for (String address: activeSubscriptions.keySet()) {
			 number += getNumberOfActiveSubscriptions(address);
		 }
		 return number;
	 }
	
	 /**
	  * Returns the Subscriber at a given address + port.
	  * @param address
	  * @param port
	  * @return the subscriber or null, if not exists
	  */
	 public synchronized Subscriber getSubscriber(String address, int port) {
		 if (subscriptionExists(address, port)) {
			 return activeSubscriptions.get(address).get(port);
		 }
		 return null;
	 }
	 
	 public synchronized void deleteAllData() {
		 activeSubscriptions = new HashMap<String, HashMap<Integer, Subscriber>>();
	 }

}
