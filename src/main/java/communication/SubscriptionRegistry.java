package communication;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import control.FBase;
import crypto.CryptoProvider.EncryptionAlgorithm;
import model.data.KeygroupID;

public class SubscriptionRegistry {

	private static Logger logger = Logger.getLogger(SubscriptionRegistry.class.getName());

	private final Map<KeygroupID, ArrayList<Subscriber>> activeSubscriptions = new HashMap<>();

	private FBase fBase;

	public SubscriptionRegistry(FBase fBase) {
		this.fBase = fBase;
	}

	/**
	 * Starts a new Subscriber and adds it to the list of active subscribers for a KeygroupID
	 * 
	 * @param address - the address to subscribe to
	 * @param port - the port to subscribe to
	 * @param secret - the secret used to decrypt received data
	 * @param algorithm - the algorithm used for decryption
	 * @param keygroupID - the related {@link KeygroupID} which is also used for filtering
	 * @return the new subscriber or null
	 */
	public synchronized Subscriber subscribeTo(String address, int port, String secret,
			EncryptionAlgorithm algorithm, KeygroupID keygroupID) {
		Subscriber subscriber = null;
		try {
			subscriber = new Subscriber(address, port, secret, algorithm, fBase, keygroupID);
			if (subscriber.startReceiving() == null) {
				throw new RuntimeException("Could not start receiving.");
			}
		} catch (Exception e) {
			logger.error("Could not initialize Subscriber");
			e.printStackTrace();
		}

		ArrayList<Subscriber> list = activeSubscriptions.get(keygroupID);
		if (list == null) {
			list = new ArrayList<Subscriber>();
			activeSubscriptions.put(keygroupID, list);
		}
		list.add(subscriber);

		return subscriber;
	}

	/**
	 * Returns the subscriber for a specific keygroup.
	 * 
	 * @param keygroupID
	 * @return see above
	 */
	public synchronized ArrayList<Subscriber> getSubscriberForKeygroup(KeygroupID keygroupID) {
		return activeSubscriptions.get(keygroupID);
	}
	
	/**
	 * Removes all subscribers from active subscriptions for a specific keygroup.
	 * NOTE: This method does not call {@link Subscriber#stopReception()}.
	 * 
	 * @param keygroupID
	 * @return all subscribers removed from active subscriptions
	 */
	public synchronized ArrayList<Subscriber> removeSubscriberForKeygroup(KeygroupID keygroupID) {
		return activeSubscriptions.remove(keygroupID);
	}

	/**
	 * Unsubscribes from all machines that this machine subscribed to for a specific keygroup.
	 * Also deletes them from the activeSubscriptions map.
	 * 
	 * @param keygroupID
	 */
	public synchronized void unsubscribeFromKeygroup(KeygroupID keygroupID) {
		for (Subscriber s : activeSubscriptions.remove(keygroupID)) {
			s.stopReception();
		}
	}

	/**
	 * Subscriptions for the given keygroupID exist, a value for the given key is present.
	 * 
	 * @param keygroupID
	 * @return true, if subscriptions exists
	 */
	public synchronized boolean subscribedToKeygroupID(KeygroupID keygroupID) {
		return activeSubscriptions.containsKey(keygroupID);
	}

	/**
	 * Returns the number of active subscriptions for a given keygroupID.
	 * 
	 * @param keygroupID
	 * @return the number or 0 if none existetn
	 */
	public synchronized int getNumberOfActiveSubscriptions(KeygroupID keygroupID) {
		if (subscribedToKeygroupID(keygroupID)) {
			return activeSubscriptions.get(keygroupID).size();
		}
		return 0;
	}

	/**
	 * Returns the number of all active subscriptions for all keygroupIDs.
	 * 
	 * @return the number or 0
	 */
	public synchronized int getNumberOfActiveSubscriptions() {
		if (activeSubscriptions.isEmpty()) {
			return 0;
		}
		int number = 0;
		for (KeygroupID keygroupID : activeSubscriptions.keySet()) {
			number += getNumberOfActiveSubscriptions(keygroupID);
		}
		return number;
	}

	/**
	 * Stops all subscriptions and deletes all data
	 */
	public synchronized void deleteAllData() {
		activeSubscriptions.values().forEach(list -> list.forEach(subs -> subs.stopReception()));
		activeSubscriptions.clear();
	}

}
