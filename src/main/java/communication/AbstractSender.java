package communication;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ;

import crypto.CryptoProvider.EncryptionAlgorithm;
import model.message.keygroup.KeygroupEnvelope;

/**
 * Abstract class for {@link Publisher} and {@link DirectSender}.
 * 
 * @author jonathanhasenburg
 *
 */
public abstract class AbstractSender {

	private static Logger logger = Logger.getLogger(AbstractSender.class.getName());
	
	/**
	 * The address used for the sending of messages.
	 */
	private String address = "";

	/**
	 * The port used for the sending of messages.
	 */
	private int port = -1;
	
	/**
	 * The secret used for the encryption of messages
	 */
	protected String secret;
	
	/**
	 * The algorithm used for the encryption of messages
	 */
	protected EncryptionAlgorithm algorithm;
	
	protected ZMQ.Context context = null;
	protected ZMQ.Socket sender = null;
	
	public AbstractSender(String address, int port, String secret, EncryptionAlgorithm algorithm, 
			int senderType) {
		this.address = address;
		this.port = port;
		this.secret = secret;
		this.algorithm = algorithm;
		this.context = ZMQ.context(1);
		if (senderType == ZMQ.PUB) {
			sender = context.socket(ZMQ.PUB);
			sender.bind(address + ":" + port);
		} else if (senderType == ZMQ.REQ) {
			sender = context.socket(ZMQ.REQ);
			sender.connect(address + ":" + port);
		} else {
			throw new IllegalArgumentException("Sender type " + senderType + " is not valid.");
		}
		logger.debug("Initialized Sender, ready to send.");
	}
	
	/**
	 * @return address used for the sending of messages
	 */
	public String getAddress() {
		return address;
	}

	/**
	 * @return port used for the sending of messages
	 */
	public int getPort() {
		return port;
	}
	
	/**
	 * Shuts down the sender. After it was shutdown, it cannot be used anymore for anything.
	 */
	public void shutdown() {
		if (sender != null) {
			sender.close();
			context.term();
			sender = null;
			logger.info("Shutdown sender, it cannot be reused.");
		}
	}
	
	public boolean isShutdown() {
		if (sender == null) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Needs to be implemented by subclasses.
	 * 
	 * @param envelope
	 * @return the answer of the request or null
	 */
	public abstract String send(KeygroupEnvelope envelope);
	
}
