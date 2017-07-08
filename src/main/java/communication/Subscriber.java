package communication;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ;

import crypto.CryptoProvider.EncryptionAlgorithm;
import model.data.DataRecord;
import model.data.KeygroupID;
import model.message.Envelope;
import model.message.Message;
import tasks.TaskManager;

/**
 * Subscribes to data streams of envelopes of different subscribers.
 * 
 * @author jonathanhasenburg
 *
 */
public class Subscriber extends AbstractReceiver {

private static Logger logger = Logger.getLogger(Subscriber.class.getName());
	
	/**
	 * Creates a subscriber, that does not filter any messages.
	 * @param address
	 * @param port
	 * @param secret
	 * @param algorithm
	 */
	public Subscriber(String address, int port, String secret, EncryptionAlgorithm algorithm) {
		super(address, port, secret, algorithm, ZMQ.SUB);
	}
	
	/**
	 * Creates a subscriber that filters all messages that do not match the keygroupIDFilter.
	 * @param address
	 * @param port
	 * @param secret
	 * @param algorithm
	 * @param keygroupIDFilter
	 */
	public Subscriber(String address, int port, String secret, EncryptionAlgorithm algorithm, 
			KeygroupID keygroupIDFilter) {
		super(address, port, secret, algorithm, ZMQ.SUB);
		this.keygroupIDFilter = keygroupIDFilter;
	}

	@Override
	protected void interpetReceivedEnvelope(Envelope envelope, ZMQ.Socket responseSocket) {
		Message m = new Message();
		try {
			// Code to interpret message
			DataRecord update = DataRecord.createFromJSON(envelope.getMessage().getContent(), DataRecord.class);
			TaskManager.runLogTask(update.toString());
			m.setTextualResponse("Message processed");
		} catch (IllegalArgumentException e) {
			logger.warn(e.getMessage());
			m.setTextualResponse(e.getMessage());
		}
		logger.debug(m.getTextualResponse());
		// Do not use the responseSocket, because a ZMQ.SUB cannot answer
	}
	
}
