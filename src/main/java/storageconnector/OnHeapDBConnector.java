/**
 * 
 */
package storageconnector;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.javatuples.Pair;

import model.config.ClientConfig;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.data.ClientID;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.KeygroupID;
import model.data.NodeID;
import exceptions.FBaseStorageConnectorException;

/**
 * This class stores all data on heap in a number of maps; it should only be used for testing
 * purposes.
 * 
 * @author Dave
 */
public class OnHeapDBConnector extends AbstractDBConnector {

	private static final Logger log = Logger.getLogger(AbstractDBConnector.class);

	/** stores keygroup config data */
	private final Map<KeygroupID, Pair<KeygroupConfig, Integer>> keygroupConfigs = new HashMap<>();

	/** stores node config data */
	private final Map<NodeID, NodeConfig> nodeConfigs = new HashMap<>();

	/** stores client config data */
	private final Map<ClientID, ClientConfig> clientConfigs = new HashMap<>();

	/** stores keygroup subscriber info */
	private final Map<KeygroupID, Pair<String, Integer>> keygroupSubscribers = new HashMap<>();

	/** stores liveness info per machine */
	private final Map<String, Long> heartbeats = new HashMap<>();

	/** stores data records */
	private final Map<KeygroupID, Map<DataIdentifier, DataRecord>> records = new HashMap<>();

	/*
	 * (non-Javadoc)
	 * 
	 * @see storageconnector.AbstractDBConnector#initiateDatabaseConnection()
	 */
	@Override
	public void dbConnection_initiate() throws FBaseStorageConnectorException {
		log.info("Connector initialized.");

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see storageconnector.AbstractDBConnector#closeDatabaseConnection()
	 */
	@Override
	public void dbConnection_close() {
		log.info("Connector closed.");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see storageconnector.AbstractDBConnector#putDataRecord(model.data.DataRecord)
	 */
	@Override
	public void dataRecords_put(DataRecord record) throws FBaseStorageConnectorException {
		Map<DataIdentifier, DataRecord> keygroupMap = records.get(record.getDataIdentifier()
				.getKeygroupID());
		if (keygroupMap == null)
			throw new FBaseStorageConnectorException("Keygroup does not exist.");
		keygroupMap.put(record.getDataIdentifier(), record);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see storageconnector.AbstractDBConnector#getDataRecord(model.data.DataIdentifier )
	 */
	@Override
	public DataRecord dataRecords_get(DataIdentifier key) throws FBaseStorageConnectorException {
		Map<DataIdentifier, DataRecord> keygroupMap = records.get(key.getKeygroupID());
		if (keygroupMap == null)
			throw new FBaseStorageConnectorException("Keygroup does not exist.");
		return keygroupMap.get(key);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see storageconnector.AbstractDBConnector#deleteDataRecord(model.data. DataIdentifier)
	 */
	@Override
	public boolean dataRecords_delete(DataIdentifier key) throws FBaseStorageConnectorException {
		Map<DataIdentifier, DataRecord> keygroupMap = records.get(key.getKeygroupID());
		if (keygroupMap == null)
			throw new FBaseStorageConnectorException("Keygroup does not exist.");
		keygroupMap.remove(key);
		return !keygroupMap.containsKey(key);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see storageconnector.AbstractDBConnector#listDataRecords(model.data.KeygroupID )
	 */
	@Override
	public Set<DataIdentifier> dataRecords_list(KeygroupID keygroup)
			throws FBaseStorageConnectorException {
		Map<DataIdentifier, DataRecord> keygroupMap = records.get(keygroup);
		if (keygroupMap == null)
			throw new FBaseStorageConnectorException("Keygroup does not exist.");
		return keygroupMap.keySet();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see storageconnector.AbstractDBConnector#createKeygroup(model.data.KeygroupID )
	 */
	@Override
	public boolean keygroup_create(KeygroupID id) throws FBaseStorageConnectorException {
		Map<DataIdentifier, DataRecord> keygroupMap = records.get(id);
		if (keygroupMap != null)
			throw new FBaseStorageConnectorException("Keygroup already exists.");
		records.put(id, new HashMap<>());
		return records.containsKey(id);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see storageconnector.AbstractDBConnector#deleteKeygroup(model.data.KeygroupID )
	 */
	@Override
	public boolean keygroup_delete(KeygroupID id) throws FBaseStorageConnectorException {
		records.remove(id);
		return !records.containsKey(id);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see storageconnector.AbstractDBConnector#putKeygroupConfig(model.data.KeygroupID ,
	 * model.config.KeygroupConfig)
	 */
	@Override
	public Integer keygroupConfig_put(KeygroupID id, KeygroupConfig config)
			throws FBaseStorageConnectorException {
		Pair<KeygroupConfig, Integer> pair = keygroupConfigs.get(id);
		if (pair == null	) {
			pair = new Pair<KeygroupConfig, Integer>(config, 1);
		} else {
			pair = pair.setAt1(pair.getValue1() + 1);
		}
		keygroupConfigs.put(id, pair);
		return pair.getValue1();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see storageconnector.AbstractDBConnector#getKeygroupConfig(model.data.KeygroupID )
	 */
	@Override
	public Pair<KeygroupConfig, Integer> keygroupConfig_get(KeygroupID keygroupID)
			throws FBaseStorageConnectorException {
		Pair<KeygroupConfig, Integer> pair = keygroupConfigs.get(keygroupID);
		if (pair == null) {
			return new Pair<KeygroupConfig, Integer>(null, null);
		} else {
			return pair;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see storageconnector.AbstractDBConnector#putNodeConfig()
	 */
	@Override
	public void nodeConfig_put(NodeID nodeID, NodeConfig config)
			throws FBaseStorageConnectorException {
		nodeConfigs.put(nodeID, config);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see storageconnector.AbstractDBConnector#getNodeConfig()
	 */
	@Override
	public NodeConfig nodeConfig_get(NodeID nodeID) throws FBaseStorageConnectorException {
		return nodeConfigs.get(nodeID);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see storageconnector.AbstractDBConnector#clientConfig_put(model.data.ClientID,
	 * model.config.ClientConfig)
	 */
	@Override
	public void clientConfig_put(ClientID clientID, ClientConfig config)
			throws FBaseStorageConnectorException {
		clientConfigs.put(clientID, config);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see storageconnector.AbstractDBConnector#clientConfig_get(model.data.ClientID)
	 */
	@Override
	public ClientConfig clientConfig_get(ClientID clientID) throws FBaseStorageConnectorException {
		return clientConfigs.get(clientID);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * storageconnector.AbstractDBConnector#keyGroupSubscriberMachines_put(model.data.KeygroupID,
	 * java.lang.String)
	 */
	@Override
	public Integer keyGroupSubscriberMachines_put(KeygroupID keygroup, String machine)
			throws FBaseStorageConnectorException {
		Pair<String, Integer> pair = keygroupSubscribers.get(keygroup);
		if (pair == null	) {
			pair = new Pair<String, Integer>(machine, 1);
		} else {
			pair = pair.setAt1(pair.getValue1() + 1);
		}
		keygroupSubscribers.put(keygroup, pair);
		return pair.getValue1();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see storageconnector.AbstractDBConnector#keyGroupSubscriberMachines_listAll()
	 */
	@Override
	public Map<KeygroupID, Pair<String, Integer>> keyGroupSubscriberMachines_listAll()
			throws FBaseStorageConnectorException {
		return new HashMap<>(keygroupSubscribers);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * storageconnector.AbstractDBConnector#keyGroupSubscriberMachines_remove(model.data.KeygroupID)
	 */
	@Override
	public void keyGroupSubscriberMachines_remove(KeygroupID keygroupid)
			throws FBaseStorageConnectorException {
		keygroupSubscribers.remove(keygroupid);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see storageconnector.AbstractDBConnector#heartbeats_update(java.lang.String)
	 */
	@Override
	public void heartbeats_update(String machine) throws FBaseStorageConnectorException {
		heartbeats.put(machine, System.currentTimeMillis());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see storageconnector.AbstractDBConnector#heartbeats_getAll()
	 */
	@Override
	public Map<String, Long> heartbeats_getAll() throws FBaseStorageConnectorException {
		return new HashMap<>(heartbeats);
	}

}
