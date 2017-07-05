package storageconnector;

import java.util.List;

import model.config.KeygroupConfig;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.KeygroupID;

// TODO David: Replace with IDBConnector

public abstract class DatabaseConnector {

	public abstract boolean initiateDatabaseConnection();
	
	public abstract boolean closeDatabaseConnection();
	
	/**
	 * Puts a data record inside the storageconnector. The record must not return null to 
	 * {@link DataRecord#getKeygroup()}, no others checks are performed.
	 * @param data - the data record to be updated
	 * @return the identifier of the data record if successful, otherwise null
	 */
	public abstract DataIdentifier putDataRecord(DataRecord data);
		
	public abstract List<DataIdentifier> listRecords(KeygroupID keygroupID, long lowTime, long highTime);
	
	public abstract DataRecord getDataRecord(DataIdentifier dataIdentifier);
	
	public abstract boolean deleteDataRecord(DataIdentifier dataIdentifier);
	
	public abstract boolean putKeygroupConfig(KeygroupConfig config);
	
	public abstract KeygroupConfig getKeygroupConfiguration(KeygroupID keygroupID);
	
	public abstract String generateNewId(KeygroupID keygroupID);
	
}
