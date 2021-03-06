package communication;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ;

import control.FBase;
import crypto.CryptoProvider.EncryptionAlgorithm;
import exceptions.FBaseEncryptionException;
import model.JSONable;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.KeygroupID;
import model.messages.Command;
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

	private String secret;
	private EncryptionAlgorithm algorithm;

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
		super(address, port, ZMQ.SUB);
		this.fBase = fBase;
		this.secret = secret;
		this.algorithm = algorithm;
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
		super(address, port, ZMQ.SUB);
		this.secret = secret;
		this.algorithm = algorithm;
		this.fBase = fBase;
		this.filterID = keygroupIDFilter;
	}

	@Override
	protected void interpreteReceivedEnvelope(Envelope envelope, ZMQ.Socket responseSocket) {
		if (fBase == null) {
			logger.warn("The subscriber is started without an fBase instance, "
					+ "so messages are not interpreted and only counted");
			return;
		}

		Message m = new Message();
		try {
			// Code to interpret message
			try {
				envelope.getMessage().decryptFields(secret, algorithm);
				Command command = envelope.getMessage().getCommand();
				String content = envelope.getMessage().getContent();
				fBase.taskmanager.runLogTask(command + " - " + content);
				if (Command.PUT_DATA_RECORD.equals(command)) {
					DataRecord update = JSONable.fromJSON(content, DataRecord.class);
					fBase.taskmanager.runPutDataRecordTask(update, false);
				} else if (Command.DELETE_DATA_RECORD.equals(command)) {
					DataIdentifier identifier = JSONable.fromJSON(content, DataIdentifier.class);
					fBase.taskmanager.runDeleteDataRecordTask(identifier, false);
				} else if (Command.UPDATE_KEYGROUP_CONFIG.equals(command)) {
					KeygroupConfig config = JSONable.fromJSON(content, KeygroupConfig.class);
					fBase.taskmanager.runUpdateKeygroupConfigTask(config, false);
				} else if (Command.UPDATE_FOREIGN_NODE_CONFIG.equals(command)) {
					NodeConfig config = JSONable.fromJSON(content, NodeConfig.class);
					fBase.taskmanager.runUpdateForeignNodeConfigTask(config);
				}

				// only add the messageID, if message was processed (e.g. could be encrypted)
				if (envelope.getMessage().getMessageID() != null) {
					fBase.messageIdEvaluator
							.addReceivedMessageID(envelope.getMessage().getMessageID());
				}
				m.setTextualInfo("Message processed");
			} catch (FBaseEncryptionException e) {
				m.setTextualInfo(
						"Could not read message with stored decryption data, " + e.getMessage());
				fBase.taskmanager.runProcessMessageWithUnknownEncryptionTask(envelope);
			}

		} catch (IllegalArgumentException e) {
			logger.warn(e.getMessage());
			m.setTextualInfo(e.getMessage());
		}
		logger.debug(m.getTextualInfo());
		// Do not use the responseSocket, because a ZMQ.SUB cannot answer
	}

}
