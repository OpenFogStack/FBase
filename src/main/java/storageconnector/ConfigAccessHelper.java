package storageconnector;

import control.FBase;
import exceptions.FBaseCommunicationException;
import exceptions.FBaseStorageConnectorException;
import model.config.ClientConfig;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.data.ClientID;
import model.data.KeygroupID;
import model.data.NodeID;

/**
 * This class should be used to access config files instead of using a DBConnector. The reason
 * for that is that this class will try to load the config from the NamingService if it cannot
 * be found in the database.
 * 
 * @author jonathanhasenburg
 *
 */
public class ConfigAccessHelper {

	private final FBase fBase;

	public ConfigAccessHelper(FBase fBase) {
		this.fBase = fBase;
	}

	/**
	 * Extends the functionality of {@link AbstractDBConnector#keygroupConfig_get(KeygroupID)}
	 * about naming service querying if not existent in node db.
	 * 
	 * @param keygroupID
	 * @return see above
	 * @throws FBaseStorageConnectorException
	 * @throws FBaseCommunicationException 
	 */
	public KeygroupConfig keygroupConfig_get(KeygroupID keygroupID)
			throws FBaseStorageConnectorException, FBaseCommunicationException {
		KeygroupConfig config = fBase.connector.keygroupConfig_get(keygroupID);
		if (config == null) {
			config = fBase.namingServiceSender.sendKeygroupConfigRead(keygroupID);
		}
		return config;
	}

	/**
	 * Extends the functionality of {@link AbstractDBConnector#nodeConfig_get(NodeID)} about
	 * naming service querying if not existent in node db.
	 * 
	 * @param nodeID
	 * @return see above
	 * @throws FBaseStorageConnectorException
	 * @throws FBaseCommunicationException
	 */
	public NodeConfig nodeConfig_get(NodeID nodeID)
			throws FBaseStorageConnectorException, FBaseCommunicationException {
		NodeConfig nodeConfig = fBase.connector.nodeConfig_get(nodeID);
		if (nodeConfig == null) {
			nodeConfig = fBase.namingServiceSender.sendNodeConfigRead(nodeID);
		}
		return nodeConfig;
	}

	/**
	 * Extends the functionality of
	 * {@link AbstractDBConnector#clientConfig_put(ClientID, ClientConfig)} about naming
	 * service querying if not existent in node db.
	 * 
	 * @param clientID
	 * @return see above
	 * @throws FBaseStorageConnectorException
	 * @throws FBaseCommunicationException
	 */
	public ClientConfig clientConfig_get(ClientID clientID)
			throws FBaseStorageConnectorException, FBaseCommunicationException {
		ClientConfig clientConfig = fBase.connector.clientConfig_get(clientID);
		if (clientConfig == null) {
			clientConfig = fBase.namingServiceSender.sendClientConfigRead(clientID);
		}
		return clientConfig;
	}

}
