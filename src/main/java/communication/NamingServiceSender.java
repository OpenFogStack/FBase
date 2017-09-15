package communication;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ;

import control.FBase;
import crypto.CryptoProvider.EncryptionAlgorithm;
import exceptions.FBaseCommunicationException;
import exceptions.FBaseEncryptionException;
import exceptions.FBaseNamingServiceException;
import model.JSONable;
import model.config.ClientConfig;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.config.ReplicaNodeConfig;
import model.config.TriggerNodeConfig;
import model.data.ClientID;
import model.data.KeygroupID;
import model.data.NodeID;
import model.messages.Command;
import model.messages.ConfigIDToKeygroupWrapper;
import model.messages.ConfigToKeygroupWrapper;
import model.messages.CryptoToKeygroupWrapper;
import model.messages.Envelope;
import model.messages.Message;
import model.messages.ResponseCode;

/**
 * Sends requests to designated receivers.
 * 
 * @author jonathanhasenburg
 *
 */
public class NamingServiceSender extends AbstractSender {

	private static Logger logger = Logger.getLogger(NamingServiceSender.class.getName());

	private FBase fBase;

	/**
	 * Initializes the NamingServiceSender, it then can be used without further modifications.
	 */
	public NamingServiceSender(String address, int port, FBase fBase) {
		super(address, port, ZMQ.REQ);
		this.fBase = fBase;
	}

	/**
	 * Sends an envelope to the specified address.
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

		String nodeID = envelope.getNodeID().getID();
		if (nodeID == null) {
			logger.error("The envelope should contain a nodeID, but does not.");
			return null;
		}

		sender.sendMore(nodeID);
		sender.send(JSONable.toJSON(envelope.getMessage()));

		ZMQ.Poller poller = context.poller();
		poller.register(sender, ZMQ.Poller.POLLIN);
		long rc = poller.poll(2000);
		if (rc == -1)
			throw new FBaseCommunicationException(
					FBaseCommunicationException.NAMING_SERVICE_NOT_REACHABLE);

		if (poller.pollin(0)) {
			// We got a reply from the server, must match sequence
			String reply = sender.recvStr();
			return reply;
		} else {
			logger.warn("Did not get a response from naming service, recreating socket");
			sender.setLinger(0); // drop pending messages when closed
			sender.close();
			poller.unregister(sender);
			sender = context.socket(ZMQ.REQ);
			sender.connect(getAddress() + ":" + getPort());
			throw new FBaseCommunicationException(
					FBaseCommunicationException.NAMING_SERVICE_NOT_REACHABLE);
		}
	}

	/**
	 * Asks the naming service to reset. Only works, if naming service is started in debug
	 * mode
	 * 
	 * @return true, if successful
	 * @throws FBaseCommunicationException
	 * @throws FBaseNamingServiceException
	 */
	public boolean sendNamingServiceReset()
			throws FBaseCommunicationException, FBaseNamingServiceException {
		Message m = new Message();
		m.setCommand(Command.RESET_NAMING_SERVICE);
		m.setContent("");
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			Message response = createDecryptedMessage(answer);
			return Boolean.parseBoolean(response.getContent());
		} catch (FBaseEncryptionException e1) {
			logger.error(e1.getMessage());
			return false;
		}
	}

	/**
	 * Asks the naming service to create a {@link NodeConfig}.
	 * 
	 * @param nodeConfig - the {@link NodeConfig}
	 * @return true, if successful
	 * @throws FBaseCommunicationException
	 * @throws FBaseNamingServiceException 
	 */
	public boolean sendNodeConfigCreate(NodeConfig nodeConfig) throws FBaseCommunicationException, FBaseNamingServiceException {
		Message m = new Message();
		m.setCommand(Command.NODE_CONFIG_CREATE);
		m.setContent(JSONable.toJSON(nodeConfig));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			Message response = createDecryptedMessage(answer);
			return Boolean.parseBoolean(response.getContent());
		} catch (FBaseEncryptionException e1) {
			logger.error(e1.getMessage());
			return false;
		}
	}

	/**
	 * Asks the naming service to update a {@link NodeConfig}.
	 * 
	 * @param nodeConfig - the {@link NodeConfig}
	 * @return true, if successful
	 * @throws FBaseCommunicationException
	 * @throws FBaseNamingServiceException 
	 */
	public boolean sendNodeConfigUpdate(NodeConfig nodeConfig) throws FBaseCommunicationException, FBaseNamingServiceException {
		Message m = new Message();
		m.setCommand(Command.NODE_CONFIG_UPDATE);
		m.setContent(JSONable.toJSON(nodeConfig));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			Message response = createDecryptedMessage(answer);
			return Boolean.parseBoolean(response.getContent());
		} catch (FBaseEncryptionException e1) {
			logger.error(e1.getMessage());
			return false;
		}
	}

	/**
	 * Ask the naming service to return the {@link NodeConfig} with the given {@link NodeID}.
	 * 
	 * @param id - the {@link NodeID}
	 * @return the specified {@link NodeConfig}
	 * @throws FBaseCommunicationException
	 * @throws FBaseNamingServiceException - if not existent
	 */
	public NodeConfig sendNodeConfigRead(NodeID id) throws FBaseCommunicationException, FBaseNamingServiceException {
		Message m = new Message();
		m.setCommand(Command.NODE_CONFIG_READ);
		m.setContent(JSONable.toJSON(id));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			Message response = createDecryptedMessage(answer);
			NodeConfig config = JSONable.fromJSON(response.getContent(), NodeConfig.class);
			return config;
		} catch (FBaseEncryptionException e1) {
			logger.error(e1.getMessage());
			return null;
		}
	}

	/**
	 * Ask the naming service to delete the {@link NodeConfig} with the given {@link NodeID}.
	 * 
	 * @param id - the {@link NodeID}
	 * @return true, if successful
	 * @throws FBaseCommunicationException
	 * @throws FBaseNamingServiceException 
	 */
	public boolean sendNodeConfigDelete(NodeID id) throws FBaseCommunicationException, FBaseNamingServiceException {
		Message m = new Message();
		m.setCommand(Command.NODE_CONFIG_DELETE);
		m.setContent(JSONable.toJSON(id));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			Message response = createDecryptedMessage(answer);
			return Boolean.parseBoolean(response.getContent());
		} catch (FBaseEncryptionException e1) {
			logger.error(e1.getMessage());
			return false;
		}
	}

	/**
	 * Asks the naming service to create a {@link ClientConfig}.
	 * 
	 * @param clientConfig - the {@link ClientConfig}
	 * @return true, if successful
	 * @throws FBaseCommunicationException
	 * @throws FBaseNamingServiceException 
	 */
	public boolean sendClientConfigCreate(ClientConfig clientConfig)
			throws FBaseCommunicationException, FBaseNamingServiceException {
		Message m = new Message();
		m.setCommand(Command.CLIENT_CONFIG_CREATE);
		m.setContent(JSONable.toJSON(clientConfig));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			Message response = createDecryptedMessage(answer);
			return Boolean.parseBoolean(response.getContent());
		} catch (FBaseEncryptionException e1) {
			logger.error(e1.getMessage());
			return false;
		}
	}

	/**
	 * Asks the naming service to update a {@link ClientConfig}.
	 * 
	 * @param clientConfig - the {@link ClientConfig}
	 * @return true, if successful
	 * @throws FBaseCommunicationException
	 * @throws FBaseNamingServiceException 
	 */
	public boolean sendClientConfigUpdate(ClientConfig clientConfig)
			throws FBaseCommunicationException, FBaseNamingServiceException {
		Message m = new Message();
		m.setCommand(Command.CLIENT_CONFIG_UPDATE);
		m.setContent(JSONable.toJSON(clientConfig));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			Message response = createDecryptedMessage(answer);
			return Boolean.parseBoolean(response.getContent());
		} catch (FBaseEncryptionException e1) {
			logger.error(e1.getMessage());
			return false;
		}
	}

	/**
	 * Ask the naming service to return the {@link ClientConfig} with the given
	 * {@link ClientID}.
	 * 
	 * @param id - the {@link ClientID}
	 * @return the specified {@link ClientConfig}
	 * @throws FBaseCommunicationException
	 * @throws FBaseNamingServiceException - if not existent 
	 */
	public ClientConfig sendClientConfigRead(ClientID id) throws FBaseCommunicationException, FBaseNamingServiceException {
		Message m = new Message();
		m.setCommand(Command.CLIENT_CONFIG_READ);
		m.setContent(JSONable.toJSON(id));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			Message response = createDecryptedMessage(answer);
			ClientConfig config = JSONable.fromJSON(response.getContent(), ClientConfig.class);
			return config;
		} catch (FBaseEncryptionException e1) {
			logger.error(e1.getMessage());
			return null;
		}
	}

	/**
	 * Ask the naming service to delete the {@link ClientConfig} with the given
	 * {@link ClientID}.
	 * 
	 * @param id - the {@link ClientID}
	 * @return true, if successful
	 * @throws FBaseCommunicationException
	 * @throws FBaseNamingServiceException 
	 */
	public boolean sendClientConfigDelete(ClientID id) throws FBaseCommunicationException, FBaseNamingServiceException {
		Message m = new Message();
		m.setCommand(Command.CLIENT_CONFIG_DELETE);
		m.setContent(JSONable.toJSON(id));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			Message response = createDecryptedMessage(answer);
			return Boolean.parseBoolean(response.getContent());
		} catch (FBaseEncryptionException e1) {
			logger.error(e1.getMessage());
			return false;
		}
	}

	/**
	 * Asks the naming service to create a {@link KeygroupConfig}.
	 * 
	 * @param config - the {@link KeygroupConfig}
	 * @return the {@link KeygroupConfig} version approved by the naming service
	 * @throws FBaseCommunicationException
	 * @throws FBaseNamingServiceException 
	 */
	public KeygroupConfig sendKeygroupConfigCreate(KeygroupConfig config)
			throws FBaseCommunicationException, FBaseNamingServiceException {
		Message m = new Message();
		m.setCommand(Command.KEYGROUP_CONFIG_CREATE);
		m.setContent(JSONable.toJSON(config));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			Message response = createDecryptedMessage(answer);
			KeygroupConfig newConfig =
					JSONable.fromJSON(response.getContent(), KeygroupConfig.class);
			return newConfig;
		} catch (FBaseEncryptionException e1) {
			logger.error(e1.getMessage());
			return null;
		}
	}

	/**
	 * Asks the naming service to add a {@link ClientID} to the {@link KeygroupConfig} with
	 * the given id
	 * 
	 * @param cID - the {@link ClientID} to be added
	 * @param keygroupID - the {@link KeygroupID}
	 * @return the {@link KeygroupConfig} version approved by the naming service
	 * @throws FBaseCommunicationException
	 * @throws FBaseNamingServiceException 
	 */
	public KeygroupConfig sendKeygroupConfigAddClient(ClientID cID, KeygroupID keygroupID)
			throws FBaseCommunicationException, FBaseNamingServiceException {
		Message m = new Message();
		m.setCommand(Command.KEYGROUP_CONFIG_ADD_CLIENT);
		ConfigIDToKeygroupWrapper<ClientID> wrapper =
				new ConfigIDToKeygroupWrapper<ClientID>(keygroupID, cID);
		m.setContent(JSONable.toJSON(wrapper));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			Message response = createDecryptedMessage(answer);
			KeygroupConfig newConfig =
					JSONable.fromJSON(response.getContent(), KeygroupConfig.class);
			return newConfig;
		} catch (FBaseEncryptionException e1) {
			logger.error(e1.getMessage());
			return null;
		}
	}

	/**
	 * Asks the naming service to delete a {@link ClientConfig} identified by an
	 * {@link ClientID} from the {@link KeygroupConfig} with the given id
	 * 
	 * @param cId - the {@link ClientID} of the to be removed config
	 * @param keygroupID - the {@link KeygroupID}
	 * @return the {@link KeygroupConfig} version approved by the naming service
	 * @throws FBaseCommunicationException
	 * @throws FBaseNamingServiceException 
	 */
	public KeygroupConfig sendKeygroupConfigDeleteClient(ClientID cId, KeygroupID keygroupID)
			throws FBaseCommunicationException, FBaseNamingServiceException {
		Message m = new Message();
		m.setCommand(Command.KEYGROUP_CONFIG_DELETE_CLIENT);
		ConfigIDToKeygroupWrapper<ClientID> wrapper =
				new ConfigIDToKeygroupWrapper<ClientID>(keygroupID, cId);
		m.setContent(JSONable.toJSON(wrapper));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			Message response = createDecryptedMessage(answer);
			KeygroupConfig newConfig =
					JSONable.fromJSON(response.getContent(), KeygroupConfig.class);
			return newConfig;
		} catch (FBaseEncryptionException e1) {
			logger.error(e1.getMessage());
			return null;
		}
	}

	/**
	 * Asks the naming service to add a {@link ReplicaNodeConfig} to the
	 * {@link KeygroupConfig} with the given id
	 * 
	 * @param rNConfig - the {@link ReplicaNodeConfig} to be added
	 * @param keygroupID - the {@link KeygroupID}
	 * @return the {@link KeygroupConfig} version approved by the naming service
	 * @throws FBaseCommunicationException
	 * @throws FBaseNamingServiceException 
	 */
	public KeygroupConfig sendKeygroupConfigAddReplicaNode(ReplicaNodeConfig rNConfig,
			KeygroupID keygroupID) throws FBaseCommunicationException, FBaseNamingServiceException {
		Message m = new Message();
		m.setCommand(Command.KEYGROUP_CONFIG_ADD_REPLICA_NODE);
		ConfigToKeygroupWrapper<ReplicaNodeConfig> wrapper =
				new ConfigToKeygroupWrapper<ReplicaNodeConfig>(keygroupID, rNConfig);
		m.setContent(JSONable.toJSON(wrapper));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			Message response = createDecryptedMessage(answer);
			KeygroupConfig newConfig =
					JSONable.fromJSON(response.getContent(), KeygroupConfig.class);
			return newConfig;
		} catch (FBaseEncryptionException e1) {
			logger.error(e1.getMessage());
			return null;
		}
	}

	/**
	 * Asks the naming service to add a {@link TriggerNodeConfig} to the
	 * {@link KeygroupConfig} with the given id
	 * 
	 * @param rNConfig - the {@link ReplicaNodeConfig} to be added
	 * @param keygroupID - the {@link KeygroupID}
	 * @return the {@link KeygroupConfig} version approved by the naming service
	 * @throws FBaseCommunicationException
	 * @throws FBaseNamingServiceException 
	 */
	public KeygroupConfig sendKeygroupConfigAddTriggerNode(TriggerNodeConfig tNConfig,
			KeygroupID keygroupID) throws FBaseCommunicationException, FBaseNamingServiceException {
		Message m = new Message();
		m.setCommand(Command.KEYGROUP_CONFIG_ADD_TRIGGER_NODE);
		ConfigToKeygroupWrapper<TriggerNodeConfig> wrapper =
				new ConfigToKeygroupWrapper<TriggerNodeConfig>(keygroupID, tNConfig);
		m.setContent(JSONable.toJSON(wrapper));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			Message response = createDecryptedMessage(answer);
			KeygroupConfig newConfig =
					JSONable.fromJSON(response.getContent(), KeygroupConfig.class);
			return newConfig;
		} catch (FBaseEncryptionException e1) {
			logger.error(e1.getMessage());
			return null;
		}
	}

	/**
	 * Ask the naming service to return the {@link KeygroupConfig} with the given
	 * {@link KeygroupID}.
	 * 
	 * @param id - the {@link KeygroupID}
	 * @return the specified {@link KeygroupConfig}
	 * @throws FBaseCommunicationException
	 * @throws FBaseNamingServiceException - if not existent
	 */
	public KeygroupConfig sendKeygroupConfigRead(KeygroupID id) throws FBaseCommunicationException, FBaseNamingServiceException {
		Message m = new Message();
		m.setCommand(Command.KEYGROUP_CONFIG_READ);
		m.setContent(JSONable.toJSON(id));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			Message response = createDecryptedMessage(answer);
			KeygroupConfig newConfig =
					JSONable.fromJSON(response.getContent(), KeygroupConfig.class);
			return newConfig;
		} catch (FBaseEncryptionException e1) {
			logger.error(e1.getMessage());
			return null;
		}
	}

	/**
	 * Asks the naming service to update the crypto information of the {@link KeygroupConfig}
	 * with the given id
	 * 
	 * @param secret - the new encryption secret
	 * @param algorithm - the new encryption algorithm
	 * @param keygroupID - the {@link KeygroupID}
	 * @return the {@link KeygroupConfig} version approved by the naming service
	 * @throws FBaseCommunicationException
	 * @throws FBaseNamingServiceException 
	 */
	public KeygroupConfig sendKeygroupConfigUpdateCrypto(String encryptionSecret,
			EncryptionAlgorithm encryptionAlgorithm, KeygroupID keygroupID)
			throws FBaseCommunicationException, FBaseNamingServiceException {
		Message m = new Message();
		m.setCommand(Command.KEYGROUP_CONFIG_UPDATE_CRYPTO);
		CryptoToKeygroupWrapper wrapper =
				new CryptoToKeygroupWrapper(keygroupID, encryptionSecret, encryptionAlgorithm);
		m.setContent(JSONable.toJSON(wrapper));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			Message response = createDecryptedMessage(answer);
			KeygroupConfig newConfig =
					JSONable.fromJSON(response.getContent(), KeygroupConfig.class);
			return newConfig;
		} catch (FBaseEncryptionException e1) {
			logger.error(e1.getMessage());
			return null;
		}
	}

	/**
	 * Ask the naming service to delete the {@link KeygroupConfig} with the given
	 * {@link KeygroupID}.
	 * 
	 * @param id - the {@link KeygroupID}
	 * @return true, if successful
	 * @throws FBaseCommunicationException
	 * @throws FBaseNamingServiceException 
	 */
	public boolean sendKeygroupConfigDelete(KeygroupID id) throws FBaseCommunicationException, FBaseNamingServiceException {
		Message m = new Message();
		m.setCommand(Command.KEYGROUP_CONFIG_DELETE);
		m.setContent(JSONable.toJSON(id));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			Message response = createDecryptedMessage(answer);
			return Boolean.parseBoolean(response.getContent());
		} catch (FBaseEncryptionException e1) {
			logger.error(e1.getMessage());
			return false;
		}
	}

	/**
	 * Ask the naming service to delete a replica or trigger node from the
	 * {@link KeygroupConfig} with the given {@link KeygroupID}.
	 * 
	 * @param keygroupID - the {@link KeygroupID}
	 * @return the {@link KeygroupConfig} version approved by the naming service
	 * @throws FBaseCommunicationException
	 * @throws FBaseNamingServiceException 
	 */
	public KeygroupConfig sendKeygroupConfigDeleteNode(KeygroupID keygroupID, NodeID nId)
			throws FBaseCommunicationException, FBaseNamingServiceException {
		Message m = new Message();
		m.setCommand(Command.KEYGROUP_CONFIG_DELETE_NODE);
		ConfigIDToKeygroupWrapper<NodeID> wrapper =
				new ConfigIDToKeygroupWrapper<NodeID>(keygroupID, nId);
		m.setContent(JSONable.toJSON(wrapper));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			Message response = createDecryptedMessage(answer);
			KeygroupConfig newConfig =
					JSONable.fromJSON(response.getContent(), KeygroupConfig.class);
			return newConfig;
		} catch (FBaseEncryptionException e1) {
			logger.error(e1.getMessage());
			return null;
		}
	}

	/**
	 * Create an envelope that is signed with the private key of the node and encrypted with
	 * the public key of the naming service. Also sets
	 * {@link Envelope#setConfigID(model.data.ConfigID)} to the {@link NodeID} of the node the
	 * machine participates with.
	 * 
	 * @param m - The message used for the envelope
	 * @return the created envelope
	 * @throws FBaseEncryptionException
	 */
	private Envelope createEncryptedEnvelope(Message m) throws FBaseEncryptionException {
		m.signMessage(fBase.configuration.getPrivateKey(), EncryptionAlgorithm.RSA);
		m.encryptFields(fBase.configuration.getNamingServicePublicKey(), EncryptionAlgorithm.RSA);
		Envelope e = new Envelope(fBase.configuration.getNodeID(), m);
		return e;
	}

	/**
	 * Creates a {@link Message} based on a given json String. Also decrypts all fields with
	 * the own private key.
	 * 
	 * @param s - json of the message
	 * @return
	 * @throws FBaseEncryptionException - if the response from the namingservice cannot be
	 *             decrypted
	 * @throws FBaseCommunicationException - if communication is not possible
	 * @throws FBaseNamingServiceException - if the response from the naming service is not
	 *             success
	 */
	private Message createDecryptedMessage(String s) throws FBaseEncryptionException,
			FBaseCommunicationException, FBaseNamingServiceException {
		if (s == null) {
			throw new FBaseCommunicationException(
					FBaseCommunicationException.NAMING_SERVICE_NOT_REACHABLE);
		}
		Message m = JSONable.fromJSON(s, Message.class);
		m.decryptFields(fBase.configuration.getPrivateKey(), EncryptionAlgorithm.RSA);
		m.verifyMessage(fBase.configuration.getNamingServicePublicKey(), EncryptionAlgorithm.RSA);
		logger.debug("Reply of naming service received");
		logger.debug("Message content: " + m.getContent());
		logger.debug("Message textual info: " + m.getTextualInfo());
		if (!m.getTextualInfo().equals(ResponseCode.SUCCESS.toString())) {
			throw new FBaseNamingServiceException(m.getTextualInfo());
		}
		return m;
	}

}
