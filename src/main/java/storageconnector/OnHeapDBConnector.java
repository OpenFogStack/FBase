/**
 * 
 */
package storageconnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.javatuples.Pair;

import exceptions.FBaseStorageConnectorException;
import model.JSONable;
import model.config.ClientConfig;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.data.ClientID;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.KeygroupID;
import model.data.MessageID;
import model.data.NodeID;

/**
 * This class stores all data on heap in a number of maps; it should only be used for testing
 * purposes.
 * 
 * 
 * @author Dave
 * @author jonathanhasenburg
 */
public class OnHeapDBConnector extends AbstractDBConnector {

	private static final Logger log = Logger.getLogger(AbstractDBConnector.class);

	/** stores keygroup config data */
	private final Map<KeygroupID, KeygroupConfig> keygroupConfigs = new HashMap<>();

	/** stores node config data */
	private final Map<NodeID, NodeConfig> nodeConfigs = new HashMap<>();

	/** stores client config data */
	private final Map<ClientID, ClientConfig> clientConfigs = new HashMap<>();

	/** stores keygroup subscriber info */
	private final Map<KeygroupID, Pair<String, Integer>> keygroupSubscribers = new HashMap<>();

	/**
	 * stores liveness info per machine <br>
	 * (machine name -> (machine address, last heartbeat in milliseconds))
	 */
	private final Map<String, Pair<String, Long>> heartbeats = new HashMap<>();

	/** stores data records */
	private final Map<KeygroupID, Map<DataIdentifier, DataRecord>> records = new HashMap<>();

	/**
	 * stores the message history
	 * 
	 * Only messageIDs from this node and machine can be in here, so we can use a small
	 * shortcut
	 */
	private final TreeMap<Integer, DataIdentifier> messageHistory = new TreeMap<>();

	private NodeID nodeID = null;
	private String machineName = null;

	public OnHeapDBConnector(NodeID nodeID) {
		this.nodeID = nodeID;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see storageconnector.AbstractDBConnector#initiateDatabaseConnection()
	 * Always returns the same name, because always only one machine exists for an on_heap node
	 */
	@Override
	public String dbConnection_initiate() throws FBaseStorageConnectorException {
		log.info("Connector initialized.");
		this.machineName = "M1";
		return this.machineName;
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
		record = JSONable.clone(record);
		Map<DataIdentifier, DataRecord> keygroupMap =
				records.get(record.getDataIdentifier().getKeygroupID());
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
		return JSONable.clone(keygroupMap.get(key));
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
		return new HashSet<DataIdentifier>(keygroupMap.keySet());
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
	public void keygroupConfig_put(KeygroupID id, KeygroupConfig config)
			throws FBaseStorageConnectorException {
		id = JSONable.clone(id);
		config = JSONable.clone(config);
		keygroupConfigs.put(id, config);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see storageconnector.AbstractDBConnector#getKeygroupConfig(model.data.KeygroupID )
	 */
	@Override
	public KeygroupConfig keygroupConfig_get(KeygroupID keygroupID)
			throws FBaseStorageConnectorException {
		return JSONable.clone(keygroupConfigs.get(keygroupID));
	}

	@Override
	public List<KeygroupID> keygroupConfig_list() throws FBaseStorageConnectorException {
		return new ArrayList<KeygroupID>(keygroupConfigs.keySet());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see storageconnector.AbstractDBConnector#putNodeConfig()
	 */
	@Override
	public void nodeConfig_put(NodeID nodeID, NodeConfig config)
			throws FBaseStorageConnectorException {
		nodeID = JSONable.clone(nodeID);
		config = JSONable.clone(config);
		nodeConfigs.put(nodeID, config);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see storageconnector.AbstractDBConnector#getNodeConfig()
	 */
	@Override
	public NodeConfig nodeConfig_get(NodeID nodeID) throws FBaseStorageConnectorException {
		return JSONable.clone(nodeConfigs.get(nodeID));
	}

	@Override
	public List<NodeID> nodeConfig_list() throws FBaseStorageConnectorException {
		return new ArrayList<NodeID>(nodeConfigs.keySet());
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
		clientID = JSONable.clone(clientID);
		config = JSONable.clone(config);
		clientConfigs.put(clientID, config);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see storageconnector.AbstractDBConnector#clientConfig_get(model.data.ClientID)
	 */
	@Override
	public ClientConfig clientConfig_get(ClientID clientID) throws FBaseStorageConnectorException {
		return JSONable.clone(clientConfigs.get(clientID));
	}

	@Override
	public List<ClientID> clientConfig_list() throws FBaseStorageConnectorException {
		return new ArrayList<ClientID>(clientConfigs.keySet());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see storageconnector.AbstractDBConnector#keyGroupSubscriberMachines_put(model.data.
	 * KeygroupID, java.lang.String)
	 */
	@Override
	public Integer keyGroupSubscriberMachines_put(KeygroupID keygroup, String machine)
			throws FBaseStorageConnectorException {
		Pair<String, Integer> pair = keygroupSubscribers.get(keygroup);
		if (pair == null) {
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
	 * @see storageconnector.AbstractDBConnector#keyGroupSubscriberMachines_remove(model.data.
	 * KeygroupID)
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
	public void heartbeats_update(String machine, String address)
			throws FBaseStorageConnectorException {
		heartbeats.put(machine, new Pair<String, Long>(address, System.currentTimeMillis()));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see storageconnector.AbstractDBConnector#heartbeats_getAll()
	 */
	@Override
	public Map<String, Pair<String, Long>> heartbeats_listAll()
			throws FBaseStorageConnectorException {
		return new HashMap<>(heartbeats);
	}
	
	@Override
	public boolean heartbeats_remove(String machine) throws FBaseStorageConnectorException {
		heartbeats.remove(machine);
		return true;
	}

	@Override
	public MessageID messageHistory_getNextMessageID() throws FBaseStorageConnectorException {
		if (messageHistory.isEmpty()) {
			return new MessageID(nodeID, machineName, 1);
		}

		int nextVersion = messageHistory.lastKey() + 1;
		return new MessageID(nodeID, machineName, nextVersion);
	}

	@Override
	public void messageHistory_put(MessageID messageID, DataIdentifier relatedData)
			throws FBaseStorageConnectorException {
		messageHistory.put(messageID.getVersion(), relatedData);
	}

	@Override
	public DataIdentifier messageHistory_get(MessageID messageID)
			throws FBaseStorageConnectorException {
		return messageHistory.get(messageID.getVersion());
	}

}
