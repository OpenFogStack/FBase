/**
 * 
 */
package storageconnector;

import java.util.Map;
import java.util.Set;

import exceptions.FBaseStorageConnectorException;
import model.data.DataIdentifier;

/**
 * 
 * TODO David: Update with methods from {@link DatabaseConnector}
 * 
 * This interface defines the operations for connecting to storageconnector systems in
 * FBase. Implementing classes define concrete behavior for actual storageconnector
 * systems.
 * 
 * <br>
 * <br>
 * <b>Please, note that subclasses must provide a no-arguments constructor which
 * will be used to instantiate them<b>
 * 
 * @author Dave
 *
 */
public interface IDBConnector {

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
	public void putItem(DataIdentifier key, Map<String, String> values)
			throws FBaseStorageConnectorException;

	/**
	 * 
	 * lists all items in the respective keygroup
	 * 
	 * @param app
	 *            first part of keygroup ID
	 * @param tenant
	 *            second part of keygroup ID
	 * @param keygroup
	 *            third part of keygroup ID
	 * @return a set of item keys or null if the specified keygroup does not
	 *         exist
	 * 
	 * @throws FBaseStorageConnectorException
	 *             when the operation failed
	 */
	public Set<String> listItems(String app, String tenant, String keygroup)
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
	public Map<String, String> getItem(DataIdentifier key)
			throws FBaseStorageConnectorException;

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
	public boolean deleteItem(DataIdentifier key)
			throws FBaseStorageConnectorException;

	/**
	 * creates a new keygroup in the storage system
	 * 
	 * @param app
	 *            first part of keygroup ID
	 * @param tenant
	 *            second part of keygroup ID
	 * @param keygroup
	 *            third part of keygroup ID
	 * @return true if the keygroup exists after the method call, false
	 *         otherwise
	 * @throws FBaseStorageConnectorException
	 */
	public boolean createKeygroup(String app, String tenant, String keygroup)
			throws FBaseStorageConnectorException;

	/**
	 * deletes a keygroup in the storage system
	 * 
	 * @param app
	 *            first part of keygroup ID
	 * @param tenant
	 *            second part of keygroup ID
	 * @param keygroup
	 *            third part of keygroup ID
	 * @return true if the keygroup no longer exists after the method call,
	 *         false otherwise
	 * @throws FBaseStorageConnectorException
	 *             when the operation failed
	 */
	public boolean deleteKeygroup(String app, String tenant, String keygroup)
			throws FBaseStorageConnectorException;

}
