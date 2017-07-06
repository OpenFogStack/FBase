/**
 * 
 */
package storageconnector;

import java.util.Map;
import java.util.Set;

import exceptions.FBaseStorageConnectorException;
import model.config.KeygroupConfig;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.KeygroupID;

/**
 * 
 * 
 * This abstract defines the operations for connecting to storageconnector
 * systems in FBase. Subclasses define concrete behavior for actual
 * storageconnector systems.
 * 
 * <br>
 * <br>
 * <b>Please, note that subclasses must provide a no-arguments constructor which
 * will be used to instantiate them<b>
 * 
 * @author Dave
 *
 */
public abstract class AbstractDBConnector {

	protected AbstractDBConnector() {
		// default constructor
	}

	/**
	 * called when initializing the connection
	 * 
	 * @throws FBaseStorageConnectorException
	 *             when something goes wrong
	 */
	public abstract void initiateDatabaseConnection()
			throws FBaseStorageConnectorException;

	/**
	 * called when closing the connection
	 * 
	 * @throws FBaseStorageConnectorException
	 *             when something goes wrong
	 */
	public abstract void closeDatabaseConnection();

	/**
	 * 
	 * @param record
	 *            the data record which shall be stored
	 * @throws FBaseStorageConnectorException
	 *             when the operation failed
	 */
	public abstract void putDataRecord(DataRecord record)
			throws FBaseStorageConnectorException;

	/**
	 * retrieve a data item for the storage system
	 * 
	 * @param key
	 *            identifier for the data item
	 * @return the respective data item if a mapping was found or null
	 * @throws FBaseStorageConnectorException
	 *             when the operation failed
	 */
	public abstract DataRecord getDataRecord(DataIdentifier key) throws FBaseStorageConnectorException;

	/**
	 * delete a data item in the storage system
	 * 
	 * @param key
	 *            identifier for the data item
	 * @return true if the item no longer exists after the method call, false
	 *         otherwise
	 * @throws FBaseStorageConnectorException
	 *             when the operation failed
	 */
	public abstract boolean deleteDataRecord(DataIdentifier key)
			throws FBaseStorageConnectorException;

	/**
	 * 
	 * lists all items in the respective keygroup
	 * 
	 * @param keygroupID
	 *            identifies the keygroup
	 * 
	 * @return a set of item keys or null if the specified keygroup does not
	 *         exist
	 * 
	 * @throws FBaseStorageConnectorException
	 *             when the operation failed
	 */
	public abstract Set<DataIdentifier> listDataRecords(KeygroupID keygroup)
			throws FBaseStorageConnectorException;

	/**
	 * creates the storage representation for a new keygroup in the storage
	 * system. 
	 * 
	 * @param id
	 *            identifier of the keygroup
	 * 
	 * @return true if the keygroup exists after the method call, false
	 *         otherwise
	 * @throws FBaseStorageConnectorException if an error occured or the keygroup already existed.
	 */
	public abstract boolean createKeygroup(KeygroupID id)
			throws FBaseStorageConnectorException;

	/**
	 * deletes all data of a keygroup in the storage system
	 * 
	 * @param id
	 *            identifier of the keygroup
	 * 
	 * @return true if the keygroup no longer exists after the method call,
	 *         false otherwise
	 * @throws FBaseStorageConnectorException
	 *             when the operation failed
	 */
	public abstract boolean deleteKeygroup(KeygroupID id)
			throws FBaseStorageConnectorException;

	/**
	 * stores configuration details of a keygroup
	 * 
	 * @param id
	 *            identifier of the keygroup
	 * @param config
	 *            configuration data
	 * @throws FBaseStorageConnectorException
	 *             when the operation fails
	 */
	public abstract void putKeygroupConfig(KeygroupID id, KeygroupConfig config)
			throws FBaseStorageConnectorException;

	/**
	 * retrieves configuration details of a keygroup
	 * 
	 * @param keygroupID
	 *            identifier of the keygroup
	 * @return the {@link KeygroupConfig} or null if none was found
	 * @throws FBaseStorageConnectorException
	 *             when the operation fails
	 */
	public abstract KeygroupConfig getKeygroupConfig(KeygroupID keygroupID)
			throws FBaseStorageConnectorException;

	/**
	 * 
	 * persists a data item in the datastore
	 * 
	 * @param key
	 *            identifier for the data item
	 * @param values
	 *            a set of values. Map keys are the column identifiers, map
	 *            values are the corresponding data entries
	 * @throws FBaseStorageConnectorException
	 *             when the operation failed
	 */
	public void putDataRecord(DataIdentifier key, Map<String, String> values)
			throws FBaseStorageConnectorException {
		putDataRecord(new DataRecord(key, values));
	}

	/**
	 * 
	 * lists all items in the respective keygroup
	 * 
	 * @param app
	 *            first part of keygroup ID
	 * @param tenant
	 *            second part of keygroup ID
	 * @param group
	 *            third part of keygroup ID
	 * @return a set of item keys or null if the specified keygroup does not
	 *         exist
	 * 
	 * @throws FBaseStorageConnectorException
	 *             when the operation failed
	 */
	public Set<DataIdentifier> listDataRecords(String app, String tenant,
			String group) throws FBaseStorageConnectorException {
		return listDataRecords(new KeygroupID(app, tenant, group));
	}

	/**
	 * creates the storage representation for a new keygroup in the storage
	 * system
	 * 
	 * @param app
	 *            first part of keygroup ID
	 * @param tenant
	 *            second part of keygroup ID
	 * @param group
	 *            third part of keygroup ID
	 * @return true if the keygroup exists after the method call, false
	 *         otherwise
	 * @throws FBaseStorageConnectorException
	 */
	public boolean createKeygroup(String app, String tenant, String group)
			throws FBaseStorageConnectorException {
		return createKeygroup(new KeygroupID(app, tenant, group));
	}

	/**
	 * deletes all data of a keygroup in the storage system
	 * 
	 * @param app
	 *            first part of keygroup ID
	 * @param tenant
	 *            second part of keygroup ID
	 * @param group
	 *            third part of keygroup ID
	 * @return true if the keygroup no longer exists after the method call,
	 *         false otherwise
	 * @throws FBaseStorageConnectorException
	 *             when the operation failed
	 */
	public boolean deleteKeygroup(String app, String tenant, String group)
			throws FBaseStorageConnectorException {
		return deleteKeygroup(new KeygroupID(app, tenant, group));
	}

}
