/**
 * 
 */
package storageconnector;

import java.util.Map;
import java.util.Set;

import exceptions.FBaseStorageConnectorException;
import model.data.DataIdentifier;

/**
 * @author Dave
 *
 */
public class DummyConnector implements IDBConnector{

	/* (non-Javadoc)
	 * @see org.openfogstack.fbase.storage.IDBConnector#putItem(model.DataIdentifier, java.util.Map)
	 */
	@Override
	public void putItem(DataIdentifier key, Map<String, String> values)
			throws FBaseStorageConnectorException {
		System.out.println("putItem works!");
	}

	/* (non-Javadoc)
	 * @see org.openfogstack.fbase.storage.IDBConnector#listItems(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public Set<String> listItems(String app, String tenant, String keygroup)
			throws FBaseStorageConnectorException {
		System.out.println("listItems works!");
		return null;
	}

	/* (non-Javadoc)
	 * @see org.openfogstack.fbase.storage.IDBConnector#getItem(model.DataIdentifier)
	 */
	@Override
	public Map<String, String> getItem(DataIdentifier key)
			throws FBaseStorageConnectorException {
		System.out.println("getItem works!");
		return null;
	}

	/* (non-Javadoc)
	 * @see org.openfogstack.fbase.storage.IDBConnector#deleteItem(model.DataIdentifier)
	 */
	@Override
	public boolean deleteItem(DataIdentifier key)
			throws FBaseStorageConnectorException {
		System.out.println("deleteItem works!");
		return false;
	}

	/* (non-Javadoc)
	 * @see org.openfogstack.fbase.storage.IDBConnector#createKeygroup(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public boolean createKeygroup(String app, String tenant, String keygroup)
			throws FBaseStorageConnectorException {
		System.out.println("createKeygroup works!");
		return false;
	}

	/* (non-Javadoc)
	 * @see org.openfogstack.fbase.storage.IDBConnector#deleteKeygroup(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public boolean deleteKeygroup(String app, String tenant, String keygroup)
			throws FBaseStorageConnectorException {
		System.out.println("deleteKeygroup works!");
		return false;
	}

}
