package communication;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ;

import crypto.CryptoProvider.EncryptionAlgorithm;
import exceptions.FBaseEncryptionException;
import model.JSONable;
import model.messages.Envelope;

/**
 * Publishes envelopes to all subscribers.
 * 
 * @author jonathanhasenburg
 *
 */
public class Publisher extends AbstractSender {

	private static Logger logger = Logger.getLogger(Publisher.class.getName());

	/**
	 * Initializes the Publisher, it then can be used without further modifications.
	 */
	public Publisher(String address, int port) {
		super(address, port, ZMQ.PUB);
	}

	/**
	 * Publishes a new envelope to all subscribers and encrypts the message with the given secret and
	 * algorithm
	 * 
	 * @param envelope - the published envelope
	 * @param secret - the secret used for encryption
	 * @param algorithm - the algorithm used for encryption
	 * @return null
	 * @throws FBaseEncryptionException 
	 */
	@Override
	public String send(Envelope envelope, String secret, EncryptionAlgorithm algorithm) throws FBaseEncryptionException {
		logger.debug("Publishing envelope with namespace " + envelope.getKeygroupID().getID());
		envelope.getMessage().encryptFields(secret, algorithm);
		sender.sendMore(envelope.getKeygroupID().getID());
		sender.send(JSONable.toJSON(envelope.getMessage()));
		return null;
	}

}
