package communication;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ;

import control.FBase;
import crypto.CryptoProvider.EncryptionAlgorithm;
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
import model.messages.ConfigToKeygroupWrapper;
import model.messages.CryptoToKeygroupWrapper;
import model.messages.Envelope;
import model.messages.Message;

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
	 * @throws FBaseNamingServiceException
	 */
	@Override
	public String send(Envelope envelope, String secret, EncryptionAlgorithm algorithm)
			throws FBaseNamingServiceException {
		sender.sendMore(envelope.getKeygroupID().getID());
		sender.send(JSONable.toJSON(envelope.getMessage()));

		ZMQ.Poller poller = context.poller();
		poller.register(sender, ZMQ.Poller.POLLIN);
		long rc = poller.poll(3000);
		if (rc == -1)
			throw new FBaseNamingServiceException(FBaseNamingServiceException.NOT_REACHABLE);

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
			throw new FBaseNamingServiceException(FBaseNamingServiceException.NOT_REACHABLE);
		}
	}

	/**
	 * Asks the naming service to create a {@link NodeConfig}.
	 * 
	 * @param nodeConfig - the {@link NodeConfig}
	 * @return true, if successful
	 */
	public boolean sendNodeConfigCreate(NodeConfig nodeConfig) {
		Message m = new Message();
		m.setCommand(Command.NODE_CONFIG_CREATE);
		m.setContent(JSONable.toJSON(nodeConfig));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			// TODO process response
			return true;
		} catch (FBaseNamingServiceException e1) {
			logger.error(e1.getMessage());
			return false;
		}
	}

	/**
	 * Asks the naming service to update a {@link NodeConfig}.
	 * 
	 * @param nodeConfig - the {@link NodeConfig}
	 * @return true, if successful
	 */
	public boolean sendNodeConfigUpdate(NodeConfig nodeConfig) {
		Message m = new Message();
		m.setCommand(Command.NODE_CONFIG_UPDATE);
		m.setContent(JSONable.toJSON(nodeConfig));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			// TODO process response
			return true;
		} catch (FBaseNamingServiceException e1) {
			logger.error(e1.getMessage());
			return false;
		}
	}

	/**
	 * Ask the naming service to return the {@link NodeConfig} with the given {@link NodeID}.
	 * 
	 * @param id - the {@link NodeID}
	 * @return the specified {@link NodeConfig} or null if not existent or service not
	 *         reachable
	 */
	public NodeConfig sendNodeConfigRead(NodeID id) {
		Message m = new Message();
		m.setCommand(Command.NODE_CONFIG_READ);
		m.setContent(JSONable.toJSON(id));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			// TODO process response
			return null;
		} catch (FBaseNamingServiceException e1) {
			logger.error(e1.getMessage());
			return null;
		}
	}

	/**
	 * Ask the naming service to delete the {@link NodeConfig} with the given {@link NodeID}.
	 * 
	 * @param id - the {@link NodeID}
	 * @return true, if successful
	 */
	public boolean sendNodeConfigDelete(NodeID id) {
		Message m = new Message();
		m.setCommand(Command.NODE_CONFIG_DELETE);
		m.setContent(JSONable.toJSON(id));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			// TODO process response
			return true;
		} catch (FBaseNamingServiceException e1) {
			logger.error(e1.getMessage());
			return false;
		}
	}

	/**
	 * Asks the naming service to create a {@link ClientConfig}.
	 * 
	 * @param clientConfig - the {@link ClientConfig}
	 * @return true, if successful
	 */
	public boolean sendClientConfigCreate(ClientConfig clientConfig) {
		Message m = new Message();
		m.setCommand(Command.CLIENT_CONFIG_CREATE);
		m.setContent(JSONable.toJSON(clientConfig));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			// TODO process response
			return true;
		} catch (FBaseNamingServiceException e1) {
			logger.error(e1.getMessage());
			return false;
		}
	}

	/**
	 * Asks the naming service to update a {@link ClientConfig}.
	 * 
	 * @param clientConfig - the {@link ClientConfig}
	 * @return true, if successful
	 */
	public boolean sendClientConfigUpdate(ClientConfig clientConfig) {
		Message m = new Message();
		m.setCommand(Command.CLIENT_CONFIG_UPDATE);
		m.setContent(JSONable.toJSON(clientConfig));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			// TODO process response
			return true;
		} catch (FBaseNamingServiceException e1) {
			logger.error(e1.getMessage());
			return false;
		}
	}

	/**
	 * Ask the naming service to return the {@link ClientConfig} with the given
	 * {@link ClientID}.
	 * 
	 * @param id - the {@link ClientID}
	 * @return the specified {@link ClientConfig} or null if not existent or service not
	 *         reachable
	 */
	public ClientConfig sendClientConfigRead(ClientID id) {
		Message m = new Message();
		m.setCommand(Command.CLIENT_CONFIG_READ);
		m.setContent(JSONable.toJSON(id));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			// TODO process response
			return null;
		} catch (FBaseNamingServiceException e1) {
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
	 */
	public boolean sendClientConfigDelete(ClientID id) {
		Message m = new Message();
		m.setCommand(Command.CLIENT_CONFIG_DELETE);
		m.setContent(JSONable.toJSON(id));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			// TODO process response
			return true;
		} catch (FBaseNamingServiceException e1) {
			logger.error(e1.getMessage());
			return false;
		}
	}

	/**
	 * Asks the naming service to create a {@link KeygroupConfig}.
	 * 
	 * @param config - the {@link KeygroupConfig}
	 * @return the {@link KeygroupConfig} version approved by the naming service
	 */
	public KeygroupConfig sendKeygroupConfigCreate(KeygroupConfig config) {
		Message m = new Message();
		m.setCommand(Command.KEYGROUP_CONFIG_CREATE);
		m.setContent(JSONable.toJSON(config));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			// TODO process response
			return null;
		} catch (FBaseNamingServiceException e1) {
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
	 */
	public KeygroupConfig sendKeygroupConfigAddReplicaNode(ReplicaNodeConfig rNConfig,
			KeygroupID keygroupID) {
		Message m = new Message();
		m.setCommand(Command.KEYGROUP_CONFIG_ADD_REPLICA_NODE);
		ConfigToKeygroupWrapper wrapper = new ConfigToKeygroupWrapper(keygroupID, rNConfig);
		m.setContent(JSONable.toJSON(wrapper));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			// TODO process response
			return null;
		} catch (FBaseNamingServiceException e1) {
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
	 */
	public KeygroupConfig sendKeygroupConfigAddTriggerNode(TriggerNodeConfig tNConfig,
			KeygroupID keygroupID) {
		Message m = new Message();
		m.setCommand(Command.KEYGROUP_CONFIG_ADD_TRIGGER_NODE);
		ConfigToKeygroupWrapper wrapper = new ConfigToKeygroupWrapper(keygroupID, tNConfig);
		m.setContent(JSONable.toJSON(wrapper));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			// TODO process response
			return null;
		} catch (FBaseNamingServiceException e1) {
			logger.error(e1.getMessage());
			return null;
		}
	}

	/**
	 * Ask the naming service to return the {@link KeygroupConfig} with the given
	 * {@link KeygroupID}.
	 * 
	 * @param id - the {@link KeygroupID}
	 * @return the specified {@link KeygroupConfig} or null if not existent or service not
	 *         reachable
	 */
	public NodeConfig sendKeygroupConfigRead(KeygroupID id) {
		Message m = new Message();
		m.setCommand(Command.KEYGROUP_CONFIG_READ);
		m.setContent(JSONable.toJSON(id));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			// TODO process response
			return null;
		} catch (FBaseNamingServiceException e1) {
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
	 */
	public KeygroupConfig sendKeygroupConfigUpdateCrypto(String encryptionSecret,
			EncryptionAlgorithm encryptionAlgorithm, KeygroupID keygroupID) {
		Message m = new Message();
		m.setCommand(Command.KEYGROUP_CONFIG_UPDATE_CRYPTO);
		CryptoToKeygroupWrapper wrapper =
				new CryptoToKeygroupWrapper(keygroupID, encryptionSecret, encryptionAlgorithm);
		m.setContent(JSONable.toJSON(wrapper));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			// TODO process response
			return null;
		} catch (FBaseNamingServiceException e1) {
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
	 */
	public boolean sendKeygroupConfigDelete(KeygroupID id) {
		Message m = new Message();
		m.setCommand(Command.KEYGROUP_CONFIG_DELETE);
		m.setContent(JSONable.toJSON(id));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			// TODO process response
			return true;
		} catch (FBaseNamingServiceException e1) {
			logger.error(e1.getMessage());
			return false;
		}
	}

	/**
	 * Ask the naming service to delete a {@link ReplicaNodeConfig} or a
	 * {@link TriggerNodeConfig} from the {@link KeygroupConfig} with the given
	 * {@link KeygroupID}.
	 * 
	 * @param keygroupID - the {@link KeygroupID}
	 * @return true, if successful
	 */
	public boolean sendKeygroupConfigDeleteNode(KeygroupID keygroupID, NodeConfig config) {
		Message m = new Message();
		m.setCommand(Command.KEYGROUP_CONFIG_DELETE_NODE);
		ConfigToKeygroupWrapper wrapper = new ConfigToKeygroupWrapper(keygroupID, config);
		m.setContent(JSONable.toJSON(wrapper));
		try {
			String answer = send(createEncryptedEnvelope(m), null, null);
			// TODO process response
			return true;
		} catch (FBaseNamingServiceException e1) {
			logger.error(e1.getMessage());
			return false;
		}
	}

	/**
	 * Create an envelope that is encrypted with the private key of the node and the public
	 * key of the naming service. Also sets {@link Envelope#setConfigID(model.data.ConfigID)}
	 * to the {@link NodeID} of the node the machine participates with.
	 * 
	 * @param m - The message used for the envelope
	 * @return the created envelope
	 */
	private Envelope createEncryptedEnvelope(Message m) {
		m.encryptFields(fBase.configuration.getPrivateKey(),
				EncryptionAlgorithm.RSA_PRIVATE_ENCRYPT);
		m.encryptFields(fBase.configuration.getNamingServicePublicKey(),
				EncryptionAlgorithm.RSA_PUBLIC_ENCRYPT);
		Envelope e = new Envelope(fBase.configuration.getNodeID(), m);
		return e;
	}

}
