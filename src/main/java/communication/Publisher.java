package communication;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ;

import crypto.CryptoProvider;
import crypto.CryptoProvider.EncryptionAlgorithm;
import model.message.keygroup.KeygroupEnvelope;

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
	public Publisher(String address, int port, String secret, EncryptionAlgorithm algorithm) {
		super(address, port, secret, algorithm, ZMQ.PUB);
	}

	/**
	 * Publishes a new envelope to all subscribers
	 * 
	 * @param envelope - the published envelope
	 * @return null
	 */
	@Override
	public String send(KeygroupEnvelope envelope) {
		logger.debug("Publishing envelope with namespace " + envelope.getKeygroupID());
		sender.sendMore(envelope.getKeygroupID().toString());
		sender.send(CryptoProvider.encrypt(envelope.getMessage().toJSON(), secret, algorithm));
		return null;
	}
	
	/**
	 * Publishes a new envelope to all subscribers
	 * 
	 * @param envelope - the published envelope
	 * @param secret - the secret used for encryption
	 * @param algorithm - the algorithm used for encryption
	 */
	public void sendKeygroupIDData(KeygroupEnvelope envelope, String secret, EncryptionAlgorithm algorithm) {
		logger.debug("Publishing envelope with namespace " + envelope.getKeygroupID());
		sender.sendMore(envelope.getKeygroupID().toString());
		sender.send(CryptoProvider.encrypt(envelope.getMessage().toJSON(), secret, algorithm));
	}
	
}
