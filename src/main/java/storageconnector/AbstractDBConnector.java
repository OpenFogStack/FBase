/**
 * 
 */
package storageconnector;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.javatuples.Pair;

import control.Configuration;
import exceptions.FBaseStorageConnectorException;
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
 * This abstract defines the operations for connecting to storageconnector systems in FBase.
 * Subclasses define concrete behavior for actual storageconnector systems. <br>
 * <br>
 * <b>Please, note that subclasses must provide a no-arguments constructor which will be used
 * to instantiate them<b>
 * 
 * @author Dave
 * @author jonathanhasenburg
 * 
 */
public abstract class AbstractDBConnector {

	public enum Connector {
		ON_HEAP, S3
	};

	protected AbstractDBConnector() {
		// default constructor
	}

	/**
	 * CONNECTION ADMINISTRATION<br<br>
	 * called when initializing the connection
	 * 
	 * @throws FBaseStorageConnectorException when something goes wrong
	 * @return the random name for the initialized machine that did not exist yet
	 */
	public abstract String dbConnection_initiate() throws FBaseStorageConnectorException;

	/**
	 * CONNECTION ADMINISTRATION<br<br>
	 * called when closing the connection
	 * 
	 * @throws FBaseStorageConnectorException when something goes wrong
	 */
	public abstract void dbConnection_close();

	/**
	 * DATA STORAGE<br>
	 * <br>
	 * 
	 * @param record the data record which shall be stored
	 * @throws FBaseStorageConnectorException when the operation failed
	 */
	public abstract void dataRecords_put(DataRecord record) throws FBaseStorageConnectorException;

	/**
	 * DATA STORAGE<br>
	 * <br>
	 * retrieve a data item for the storage system
	 * 
	 * @param dataIdentifier identifier for the data item
	 * @return the respective data item if a mapping was found or null
	 * @throws FBaseStorageConnectorException when the operation failed
	 */
	public abstract DataRecord dataRecords_get(DataIdentifier dataIdentifier)
			throws FBaseStorageConnectorException;

	/**
	 * DATA STORAGE<br>
	 * <br>
	 * delete a data item in the storage system
	 * 
	 * @param dataIdentifier identifier for the data item
	 * @return true if the item no longer exists after the method call, false otherwise
	 * @throws FBaseStorageConnectorException when the operation failed
	 */
	public abstract boolean dataRecords_delete(DataIdentifier dataIdentifier)
			throws FBaseStorageConnectorException;

	/**
	 * DATA STORAGE<br>
	 * <br>
	 * lists all items in the respective keygroup
	 * 
	 * @param keygroupID identifies the keygroup
	 * @return a set of item keys or null if the specified keygroup does not exist
	 * @throws FBaseStorageConnectorException when the operation failed
	 */
	public abstract Set<DataIdentifier> dataRecords_list(KeygroupID keygroupID)
			throws FBaseStorageConnectorException;

	/**
	 * KEYGROUP MANAGEMENT<br>
	 * <br>
	 * creates the storage representation for a new keygroup in the storage system.
	 * 
	 * @param keygroupID identifier of the keygroup
	 * @return true if the keygroup exists after the method call, false otherwise
	 * @throws FBaseStorageConnectorException if an error occured or the keygroup already
	 *             existed.
	 */
	public abstract boolean keygroup_create(KeygroupID keygroupID)
			throws FBaseStorageConnectorException;

	/**
	 * KEYGROUP MANAGEMENT<br>
	 * <br>
	 * deletes all data of a keygroup in the storage system
	 * 
	 * @param keygroupID identifier of the keygroup
	 * @return true if the keygroup no longer exists after the method call, false otherwise
	 * @throws FBaseStorageConnectorException when the operation failed
	 */
	public abstract boolean keygroup_delete(KeygroupID keygroupID)
			throws FBaseStorageConnectorException;

	/**
	 * KEYGROUP CONFIG<br>
	 * <br>
	 * Stores configuration details of a keygroup
	 * 
	 * @param keygroupID identifier of the keygroup
	 * @param config configuration data
	 * @throws FBaseStorageConnectorException when the operation fails
	 */
	public abstract void keygroupConfig_put(KeygroupID keygroupID, KeygroupConfig config)
			throws FBaseStorageConnectorException;

	/**
	 * KEYGROUP CONFIG<br>
	 * <br>
	 * Retrieves configuration details of a keygroup as stored by keygroupConfig_put()
	 * 
	 * @param keygroupID identifier of the keygroup
	 * @return the {@link KeygroupConfig}
	 * @throws FBaseStorageConnectorException when the operation fails
	 */
	public abstract KeygroupConfig keygroupConfig_get(KeygroupID keygroupID)
			throws FBaseStorageConnectorException;

	/**
	 * 
	 * KEYGROUP CONFIG<br>
	 * <br>
	 * 
	 * Retrieves the {@link KeygroupID}s for all stored {@link KeygroupConfig}s.
	 * 
	 * @return a list of {@link KeygroupID}s
	 * @throws FBaseStorageConnectorException
	 */
	public abstract List<KeygroupID> keygroupConfig_list() throws FBaseStorageConnectorException;

	/**
	 * NODE CONFIG<br>
	 * <br>
	 * stores configuration details of a node
	 * 
	 * @param nodeID identifier of the node
	 * @param config configuration data
	 * @throws FBaseStorageConnectorException when the operation fails
	 */
	public abstract void nodeConfig_put(NodeID nodeID, NodeConfig config)
			throws FBaseStorageConnectorException;

	/**
	 * NODE CONFIG<br>
	 * <br>
	 * retrieves configuration details of a node
	 * 
	 * @param keygroupID identifier of the node
	 * @return the {@link NodeConfig} or null if none was found
	 * @throws FBaseStorageConnectorException when the operation fails
	 */
	public abstract NodeConfig nodeConfig_get(NodeID nodeID) throws FBaseStorageConnectorException;

	/**
	 * 
	 * NODE CONFIG<br>
	 * <br>
	 * 
	 * Retrieves the {@link NodeID}s for all stored {@link NodeConfig}s.
	 * 
	 * @return a list of {@link NodeID}s
	 * @throws FBaseStorageConnectorException
	 */
	public abstract List<NodeID> nodeConfig_list() throws FBaseStorageConnectorException;

	/**
	 * CLIENT CONFIG<br>
	 * <br>
	 * stores configuration details of a client
	 * 
	 * @param clientID identifier of the client
	 * @param config configuration data
	 * @throws FBaseStorageConnectorException when the operation fails
	 */
	public abstract void clientConfig_put(ClientID clientID, ClientConfig config)
			throws FBaseStorageConnectorException;

	/**
	 * CLIENT CONFIG<br>
	 * <br>
	 * retrieves configuration details of a client
	 * 
	 * @param keygroupID identifier of the node
	 * @return the {@link NodeConfig} or null if none was found
	 * @throws FBaseStorageConnectorException when the operation fails
	 */
	public abstract ClientConfig clientConfig_get(ClientID clientID)
			throws FBaseStorageConnectorException;

	/**
	 * 
	 * CLIENT CONFIG<br>
	 * <br>
	 * 
	 * Retrieves the {@link ClientID}s for all stored {@link ClientConfigs}s.
	 * 
	 * @return a list of {@link ClientID}s
	 * @throws FBaseStorageConnectorException
	 */
	public abstract List<ClientID> clientConfig_list() throws FBaseStorageConnectorException;

	/**
	 * SUBSCRIBER MANAGEMENT<br>
	 * <br>
	 * Puts into the database which machine of the node subscribes to a given keygroup. Each
	 * keygroup - machine tuple has a version (integer), which is incremented upon each put
	 * operation
	 * 
	 * @param keygroup
	 * @param machine
	 * @return the new version of the keygroup - machine tuple
	 * @throws FBaseStorageConnectorException when the operation fails
	 */
	public abstract Integer keyGroupSubscriberMachines_put(KeygroupID keygroup, String machine)
			throws FBaseStorageConnectorException;

	/**
	 * SUBSCRIBER MANAGEMENT<br>
	 * <br>
	 * 
	 * @return all mappings for keygroup ids to their respective subscriber machines as stored
	 *         by keyGroupSubscriberMachines_put()
	 * @throws FBaseStorageConnectorException when the operation fails
	 */
	public abstract Map<KeygroupID, Pair<String, Integer>> keyGroupSubscriberMachines_listAll()
			throws FBaseStorageConnectorException;

	/**
	 * SUBSCRIBER MANAGEMENT<br>
	 * <br>
	 * removes a keygroup id to machine mapping
	 * 
	 * @param keygroupid keygroup id for which the mapping shall be removed
	 * @throws FBaseStorageConnectorException when the operation fails
	 */
	public abstract void keyGroupSubscriberMachines_remove(KeygroupID keygroupid)
			throws FBaseStorageConnectorException;

	/**
	 * HEARTBEATS<br>
	 * <br>
	 * sets the "I'm alive" timestamp for the specified machine to the current time
	 * 
	 * @param machine - the machine's name
	 * @param address - the machine's address
	 * @throws FBaseStorageConnectorException when the operation fails
	 */
	public abstract void heartbeats_update(String machine, String address)
			throws FBaseStorageConnectorException;

	/**
	 * HEARTBEATS<br>
	 * <br>
	 * 
	 * @return a mapping of all machine names and their addresses within this node and the last
	 *         time they each reported to be alive.
	 * @throws FBaseStorageConnectorException when the operation fails
	 */
	public abstract Map<String, Pair<String, Long>> heartbeats_listAll()
			throws FBaseStorageConnectorException;
	
	/**
	 * HEARTBEATS<br>
	 * <br>
	 * removes a machine from the heartbeats table.
	 * 
	 * @param machine - the machine's name
	 * @return true if the item no longer exists after the method call, false otherwise
	 * @throws FBaseStorageConnectorException when the operation fails
	 */
	public abstract boolean heartbeats_remove(String machine)
			throws FBaseStorageConnectorException;

	/**
	 * MESSAGEHISTORY<br>
	 * <br>
	 * 
	 * Uses {@link Configuration#getNodeID()} and {@link Configuration#getMachineName()} to
	 * determine the nodeID and the machineName needed for the messageID
	 * 
	 * @return the next {@link MessageID}
	 * @throws FBaseStorageConnectorException when the operation fails
	 */
	public abstract MessageID messageHistory_getNextMessageID()
			throws FBaseStorageConnectorException;

	/**
	 * MESSAGEHISTORY<br>
	 * <br>
	 * puts something in the messageHistory.
	 * 
	 * @param messageID - the {@link MessageID}
	 * @param relatedData - the {@link DataIdentifier} of the data item the original message
	 *            related to
	 * 
	 * @throws FBaseStorageConnectorException when the operation fails
	 */
	public abstract void messageHistory_put(MessageID messageID, DataIdentifier relatedData)
			throws FBaseStorageConnectorException;

	/**
	 * MESSAGEHISTORY<br>
	 * <br>
	 * gets something in the messageHistory.
	 * 
	 * @param messageID - the {@link MessageID}
	 * 
	 * @return the {@link DataIdentifier} related to the given messageID or null no entry for
	 *         the messageID exists
	 * @throws FBaseStorageConnectorException when the operation fails
	 */
	public abstract DataIdentifier messageHistory_get(MessageID messageID)
			throws FBaseStorageConnectorException;

	/*
	 * 
	 * Convenience methods start here
	 */

	/**
	 * DATA STORAGE<br>
	 * <br>
	 * persists a data item in the datastore
	 * 
	 * @param key identifier for the data item
	 * @param values a set of values. Map keys are the column identifiers, map values are the
	 *            corresponding data entries
	 * @throws FBaseStorageConnectorException when the operation failed
	 */
	public void dataRecords_put(DataIdentifier key, Map<String, String> values)
			throws FBaseStorageConnectorException {
		dataRecords_put(new DataRecord(key, values));
	}

	/**
	 * DATA STORAGE<br>
	 * <br>
	 * lists all items in the respective keygroup
	 * 
	 * @param app first part of keygroup ID
	 * @param tenant second part of keygroup ID
	 * @param group third part of keygroup ID
	 * @return a set of item keys or null if the specified keygroup does not exist
	 * @throws FBaseStorageConnectorException when the operation failed
	 */
	public Set<DataIdentifier> dataRecords_list(String app, String tenant, String group)
			throws FBaseStorageConnectorException {
		return dataRecords_list(new KeygroupID(app, tenant, group));
	}

	/**
	 * KEYGROUP MANAGEMENT<br>
	 * <br>
	 * creates the storage representation for a new keygroup in the storage system
	 * 
	 * @param app first part of keygroup ID
	 * @param tenant second part of keygroup ID
	 * @param group third part of keygroup ID
	 * @return true if the keygroup exists after the method call, false otherwise
	 * @throws FBaseStorageConnectorException
	 */
	public boolean keygroup_create(String app, String tenant, String group)
			throws FBaseStorageConnectorException {
		return keygroup_create(new KeygroupID(app, tenant, group));
	}

	/**
	 * KEYGROUP MANAGEMENT<br>
	 * <br>
	 * deletes all data of a keygroup in the storage system
	 * 
	 * @param app first part of keygroup ID
	 * @param tenant second part of keygroup ID
	 * @param group third part of keygroup ID
	 * @return true if the keygroup no longer exists after the method call, false otherwise
	 * @throws FBaseStorageConnectorException when the operation failed
	 */
	public boolean keygroup_delete(String app, String tenant, String group)
			throws FBaseStorageConnectorException {
		return keygroup_delete(new KeygroupID(app, tenant, group));
	}
	
	protected String generateNameString(int size) {
		String possibleCharacterString = "abcdefghijklmnopqrstuvwxyz";
		char[] chars = possibleCharacterString.toCharArray();
		StringBuilder builder = new StringBuilder();
		Random random = new Random();
		for (int i = 0; i < size; i++) {
			char c = chars[random.nextInt(chars.length)];
			builder.append(c);
		}
		String output = builder.toString();
		return output;
	}

}
