package tasks;

import org.apache.log4j.Logger;

import control.FBase;
import exceptions.FBaseStorageConnectorException;
import model.config.KeygroupConfig;
import model.data.DataRecord;
import model.message.keygroup.KeygroupEnvelope;
import model.message.keygroup.KeygroupMessage;
import tasks.TaskManager.TaskName;

/**
 * This task takes a {@link DataRecord} and puts it into the local database using the default database connector.
 * Furthermore, it instructs the publisher to publish the given data record.
 * 
 * @author jonathanhasenburg
 */
class PutDataRecordTask extends Task<Boolean> {

	private static Logger logger = Logger.getLogger(PutDataRecordTask.class.getName());
	
	private DataRecord record;

	public PutDataRecordTask(DataRecord record, TaskManager taskmanager) {
		super(TaskName.PUT_DATA_RECORD, taskmanager);
		this.record = record;
	}
	
	/**
	 * Put a {@link DataRecord} into the local database and instruct the publisher to publish it.
	 * Fails, if no keygroup exists for the given record, or no related keygroup config is found.
	 * 
	 * @return true, if everything works, otherwise false
	 */
	@Override
	public Boolean executeFunctionality() {
		// get keygroup config and put into database
		KeygroupConfig config = null;	
		try {
			config = FBase.connector.getKeygroupConfig(record.getKeygroupID());
			FBase.connector.putDataRecord(record);
		} catch (FBaseStorageConnectorException e) {
			logger.error(e.getMessage());
			return false;
		}
		
		// create envelope
		KeygroupMessage m = new KeygroupMessage();
		m.setContent(record.toJSON());
		KeygroupEnvelope e = new KeygroupEnvelope(record.getKeygroupID(), m);
		
		// publish data
		FBase.publisher.sendKeygroupIDData(e, config.getEncryptionSecret(), config.getEncryptionAlgorithm());
		
		return true;
	}

	

}
