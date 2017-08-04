package communication;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ;

import control.FBase;
import crypto.CryptoProvider.EncryptionAlgorithm;
import model.JSONable;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.KeygroupID;
import model.messages.Envelope;
import model.messages.Message;

/**
 * Subscribes to data streams of envelopes of different subscribers.
 * 
 * @author jonathanhasenburg
 *
 */
public class Subscriber extends AbstractReceiver {

	private static Logger logger = Logger.getLogger(Subscriber.class.getName());
	
	private FBase fBase;

	/**
	 * Creates a subscriber, that does not filter any messages.
	 * 
	 * @param address
	 * @param port
	 * @param secret
	 * @param algorithm
	 */
	public Subscriber(String address, int port, String secret, EncryptionAlgorithm algorithm,
			FBase fBase) {
		super(address, port, secret, algorithm, ZMQ.SUB);
		this.fBase = fBase;
	}

	/**
	 * Creates a subscriber that filters all messages that do not match the keygroupIDFilter.
	 * 
	 * @param address
	 * @param port
	 * @param secret
	 * @param algorithm
	 * @param keygroupIDFilter
	 */
	public Subscriber(String address, int port, String secret, EncryptionAlgorithm algorithm,
			FBase fBase, KeygroupID keygroupIDFilter) {
		super(address, port, secret, algorithm, ZMQ.SUB);
		this.fBase = fBase;
		this.filterID = keygroupIDFilter;
	}

	@Override
	protected void interpreteReceivedEnvelope(Envelope envelope, ZMQ.Socket responseSocket) {
		Message m = new Message();
		try {
			// Code to interpret message
			fBase.taskmanager.runLogTask(envelope.getMessage().getTextualResponse() + " - "
					+ envelope.getMessage().getContent());
			if ("PUT".equals(envelope.getMessage().getTextualResponse())) {
				DataRecord update = JSONable.fromJSON(envelope.getMessage().getContent(),
						DataRecord.class);
				fBase.taskmanager.runStoreDataRecordTask(update);
			} else if ("DELETE".equals(envelope.getMessage().getTextualResponse())) {
				DataIdentifier identifier = JSONable.fromJSON(envelope.getMessage().getContent(),
						DataIdentifier.class);
				fBase.taskmanager.runDeleteDataRecordTask(identifier);
			}
			m.setTextualResponse("Message processed");
		} catch (IllegalArgumentException e) {
			logger.warn(e.getMessage());
			m.setTextualResponse(e.getMessage());
		}
		logger.debug(m.getTextualResponse());
		// Do not use the responseSocket, because a ZMQ.SUB cannot answer
	}

}
