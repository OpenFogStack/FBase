package storageconnector;

import org.javatuples.Pair;

import control.FBase;
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
 * TODO I: Incorporate communication with NamingService
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
	 */
	public Pair<KeygroupConfig, Integer> keygroupConfig_get(KeygroupID keygroupID)
			throws FBaseStorageConnectorException {
		return fBase.connector.keygroupConfig_get(keygroupID);
	}

	/**
	 * Extends the functionality of {@link AbstractDBConnector#nodeConfig_get(NodeID)} about
	 * naming service querying if not existent in node db.
	 * 
	 * @param nodeID
	 * @return see above
	 * @throws FBaseStorageConnectorException
	 */
	public NodeConfig nodeConfig_get(NodeID nodeID) throws FBaseStorageConnectorException {
		return fBase.connector.nodeConfig_get(nodeID);
	}

	/**
	 * Extends the functionality of
	 * {@link AbstractDBConnector#clientConfig_put(ClientID, ClientConfig)} about naming
	 * service querying if not existent in node db.
	 * 
	 * @param clientID
	 * @return see above
	 * @throws FBaseStorageConnectorException
	 */
	public ClientConfig clientConfig_get(ClientID clientID) throws FBaseStorageConnectorException {
		return fBase.connector.clientConfig_get(clientID);
	}

}
