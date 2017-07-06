/**
 * 
 */
package storageconnector;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import model.config.KeygroupConfig;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.KeygroupID;
import exceptions.FBaseStorageConnectorException;

/**
 * This class stores all data on heap in a number of maps; it should only be
 * used for testing purposes.
 * 
 * @author Dave
 *
 */
public class OnHeapDBConnector extends AbstractDBConnector {

	private static final Logger log = Logger
			.getLogger(AbstractDBConnector.class);

	/** stores keygroup config data */
	private final Map<KeygroupID, KeygroupConfig> configs = new HashMap<>();

	/** stores data records */
	private final Map<KeygroupID, Map<DataIdentifier, DataRecord>> records = new HashMap<>();

	/*
	 * (non-Javadoc)
	 * 
	 * @see storageconnector.AbstractDBConnector#initiateDatabaseConnection()
	 */
	@Override
	public void initiateDatabaseConnection()
			throws FBaseStorageConnectorException {
		log.info("Connector initialized.");

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see storageconnector.AbstractDBConnector#closeDatabaseConnection()
	 */
	@Override
	public void closeDatabaseConnection() {
		log.info("Connector closed.");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * storageconnector.AbstractDBConnector#putDataRecord(model.data.DataRecord)
	 */
	@Override
	public void putDataRecord(DataRecord record)
			throws FBaseStorageConnectorException {
		Map<DataIdentifier, DataRecord> keygroupMap = records.get(record
				.getDataIdentifier().getKeygroup());
		if (keygroupMap == null)
			throw new FBaseStorageConnectorException(
					"Keygroup does not exist.");
		keygroupMap.put(record.getDataIdentifier(), record);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * storageconnector.AbstractDBConnector#getDataRecord(model.data.DataIdentifier
	 * )
	 */
	@Override
	public DataRecord getDataRecord(DataIdentifier key) throws FBaseStorageConnectorException{
		Map<DataIdentifier, DataRecord> keygroupMap = records.get(key.getKeygroup());
		if (keygroupMap == null)
			throw new FBaseStorageConnectorException(
					"Keygroup does not exist.");
		return keygroupMap.get(key);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see storageconnector.AbstractDBConnector#deleteDataRecord(model.data.
	 * DataIdentifier)
	 */
	@Override
	public boolean deleteDataRecord(DataIdentifier key)
			throws FBaseStorageConnectorException {
		Map<DataIdentifier, DataRecord> keygroupMap = records.get(key.getKeygroup());
		if (keygroupMap == null)
			throw new FBaseStorageConnectorException(
					"Keygroup does not exist.");
		keygroupMap.remove(key);
		return !keygroupMap.containsKey(key);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * storageconnector.AbstractDBConnector#listDataRecords(model.data.KeygroupID
	 * )
	 */
	@Override
	public Set<DataIdentifier> listDataRecords(KeygroupID keygroup)
			throws FBaseStorageConnectorException {
		Map<DataIdentifier, DataRecord> keygroupMap = records.get(keygroup);
		if (keygroupMap == null)
			throw new FBaseStorageConnectorException(
					"Keygroup does not exist.");
		return keygroupMap.keySet();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * storageconnector.AbstractDBConnector#createKeygroup(model.data.KeygroupID
	 * )
	 */
	@Override
	public boolean createKeygroup(KeygroupID id)
			throws FBaseStorageConnectorException {
		Map<DataIdentifier, DataRecord> keygroupMap = records.get(id);
		if(keygroupMap!=null)throw new FBaseStorageConnectorException(
				"Keygroup already exists.");
		records.put(id, new HashMap<>());
		return records.containsKey(id);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * storageconnector.AbstractDBConnector#deleteKeygroup(model.data.KeygroupID
	 * )
	 */
	@Override
	public boolean deleteKeygroup(KeygroupID id)
			throws FBaseStorageConnectorException {
		records.remove(id);
		return !records.containsKey(id);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * storageconnector.AbstractDBConnector#putKeygroupConfig(model.data.KeygroupID
	 * , model.config.KeygroupConfig)
	 */
	@Override
	public void putKeygroupConfig(KeygroupID id, KeygroupConfig config)
			throws FBaseStorageConnectorException {
		configs.put(id, config);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * storageconnector.AbstractDBConnector#getKeygroupConfig(model.data.KeygroupID
	 * )
	 */
	@Override
	public KeygroupConfig getKeygroupConfig(KeygroupID keygroupID)
			throws FBaseStorageConnectorException {
		return configs.get(keygroupID);
	}

}
