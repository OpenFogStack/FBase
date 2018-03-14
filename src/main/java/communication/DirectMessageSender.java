package communication;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ;

import control.FBase;
import crypto.CryptoProvider.EncryptionAlgorithm;
import de.hasenburg.fbase.model.GetMissedMessageResponse;
import exceptions.FBaseCommunicationException;
import exceptions.FBaseEncryptionException;
import model.JSONable;
import model.config.NodeConfig;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.MessageID;
import model.messages.Command;
import model.messages.Envelope;
import model.messages.Message;

/**
 * Sends requests to designated receivers.
 * 
 * @author jonathanhasenburg
 *
 */
public class DirectMessageSender extends AbstractSender {

	private static Logger logger = Logger.getLogger(DirectMessageSender.class.getName());

	private FBase fBase = null;
	private NodeConfig targetNode = null;

	/**
	 * Initializes the Message, it then can be used without further modifications.
	 * Messages are send to a random machine of the given node configuration.
	 */
	public DirectMessageSender(NodeConfig targetNode, FBase fBase) {
		super(getRandomAddress(targetNode.getMachines()), targetNode.getMessagePort(), ZMQ.REQ);
		this.fBase = fBase;
		this.targetNode = targetNode;
	}

	/**
	 * Initializes the Message, it then can be used without further modifications.
	 */
	public DirectMessageSender(String targetAddress, int targetPort, FBase fBase) {
		super(targetAddress, targetPort, ZMQ.REQ);
		this.fBase = fBase;
	}
	
	private static String getRandomAddress(List<String> machines) {
		int randomNum = ThreadLocalRandom.current().nextInt(0, machines.size());
		return "tcp://" + machines.get(randomNum);
	}

	/**
	 * Sends an envelope to the address of the sender. The parameters secret and algorithm are
	 * not used. If the envelope should contain an encrypted message, it must be supplied
	 * first.
	 * 
	 * It is recommended to use the other sender methods instead of this one directly
	 * 
	 * @param envelope
	 * @return the response
	 * @throws FBaseCommunicationException
	 */
	@Override
	public String send(Envelope envelope, String secret, EncryptionAlgorithm algorithm)
			throws FBaseCommunicationException {

		if (!ableToSend) {
			return null;
		}

		sender.sendMore(envelope.getConfigID().getID());
		sender.send(JSONable.toJSON(envelope.getMessage()));

		ZMQ.Poller poller = context.poller();
		poller.register(sender, ZMQ.Poller.POLLIN);
		long rc = poller.poll(2000);
		if (rc == -1)
			throw new FBaseCommunicationException(FBaseCommunicationException.NODE_NOT_REACHABLE);

		if (poller.pollin(0)) {
			// We got a reply from the server, must match sequence
			String reply = sender.recvStr();
			return reply;
		} else {
			logger.warn("Did not get a response from node, recreating socket");
			sender.setLinger(0); // drop pending messages when closed
			sender.close();
			poller.unregister(sender);
			sender = context.socket(ZMQ.REQ);
			sender.connect(getAddress() + ":" + getPort());
			throw new FBaseCommunicationException(FBaseCommunicationException.NODE_NOT_REACHABLE);
		}
	}

	/**
	 * Ask the target node to return the {@link DataIdentifier} and {@link DataRecord} which
	 * is related to the given {@link MessageID}. The dataRecord might be null if it was
	 * deleted.
	 * 
	 * @param messageID - the {@link MessageID}
	 * @return the specified values wrapped in {@link GetMissedMessageResponse}
	 * @throws FBaseCommunicationException
	 */
	public GetMissedMessageResponse sendGetDataRecord(MessageID messageID)
			throws FBaseCommunicationException {
		Message m = new Message();
		m.setCommand(Command.GET_DATA_FOR_MESSAGEID);
		m.setContent(messageID.getMessageIDString());
		try {
			String answer = send(createEncryptedEnvelope(m, targetNode.getPublicKey()), null, null);
			Message response = createDecryptedMessage(answer, targetNode.getPublicKey());

			GetMissedMessageResponse returnVal;

			if (response.getContent() != null) {
				returnVal =
						JSONable.fromJSON(response.getContent(), GetMissedMessageResponse.class);
			} else {
				returnVal = new GetMissedMessageResponse();
			}
			returnVal.setTextualInfo(response.getTextualInfo());

			return returnVal;
		} catch (FBaseEncryptionException e1) {
			logger.error(e1.getMessage(), e1);
			return null;
		}
	}

	/**
	 * Ask the target machine to run the test of {@link tasks.AnnounceUpdateOfOwnNodeConfigurationTask}.
	 * This is necessary if a new machine was added to a node, because node configuration
	 * updates are distributed via the publisher to other nodes (and other nodes have not
	 * subscribed to the new machine yet).
	 * 
	 * @throws FBaseCommunicationException - if other node declines/cannot be reached
	 */
	public void sendAnnounceMeRequest() throws FBaseCommunicationException {
		Message m = new Message();
		m.setContent(Command.ANNOUNCE_OWN_NODE_CONFIGURATION_CHANGE.toString());
		m.setCommand(Command.ANNOUNCE_OWN_NODE_CONFIGURATION_CHANGE);
		try {
			String answer = send(createEncryptedEnvelope(m, fBase.configuration.getPublicKey()), null, null);
			Message response = createDecryptedMessage(answer, fBase.configuration.getPublicKey());

			if (response.getContent().equals("true")) {
				logger.debug("The other machine announced me");
				return;
			} else {
				logger.debug("The other machine could not announce me");
				throw new FBaseCommunicationException("The other machine could not announce me " + response.getContent());
			}
		} catch (FBaseEncryptionException e1) {
			logger.error(e1.getMessage(), e1);
			throw new FBaseCommunicationException("Announce Request failed " + e1.getMessage());
		}
	}

	/**
	 * Create an envelope that is signed with the private key of the node and encrypted with
	 * the public key of the target node. Also sets
	 * {@link Envelope#setConfigID(model.data.ConfigID)} to this node's nodeID.
	 * 
	 * @param m - The message used for the envelope
	 * @param nodePublicKey - the public key of the target node
	 * @return the created envelope
	 * @throws FBaseEncryptionException
	 */
	private Envelope createEncryptedEnvelope(Message m, String nodePublicKey)
			throws FBaseEncryptionException {
		m.signMessage(fBase.configuration.getPrivateKey(), EncryptionAlgorithm.RSA);
		m.encryptFields(nodePublicKey, EncryptionAlgorithm.RSA);
		Envelope e = new Envelope(fBase.configuration.getNodeID(), m);
		return e;
	}

	/**
	 * Creates a {@link Message} based on a given json String. Also decrypts all fields with
	 * the own private key and verifies the signature with the provided public key of a node.
	 * 
	 * @param s - json of the message
	 * @param nodePublicKey - the public key of the target node
	 * 
	 * @return the decrypted message
	 * @throws FBaseEncryptionException
	 * @throws FBaseCommunicationException
	 */
	private Message createDecryptedMessage(String s, String nodePublicKey)
			throws FBaseEncryptionException, FBaseCommunicationException {
		if (s == null) {
			throw new FBaseCommunicationException(FBaseCommunicationException.NODE_NOT_REACHABLE);
		}
		Message m = JSONable.fromJSON(s, Message.class);
		m.decryptFields(fBase.configuration.getPrivateKey(), EncryptionAlgorithm.RSA);
		if (m.getContent() != null) {
			// we can only verify, if a content was set
			m.verifyMessage(nodePublicKey, EncryptionAlgorithm.RSA);
		}
		logger.debug("Reply of node received");
		logger.debug("Message content: " + m.getContent());
		logger.debug("Message textual info: " + m.getTextualInfo());
		return m;
	}

}
