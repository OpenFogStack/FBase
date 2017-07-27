package tasks;

import org.apache.log4j.Logger;

import control.FBase;
import exceptions.FBaseStorageConnectorException;
import model.data.DataRecord;
import tasks.TaskManager.TaskName;

/**
 * This task takes a {@link DataRecord} and puts it into the local database using the default
 * database connector.
 * 
 * @author jonathanhasenburg
 *
 */
class StoreDataRecordTask extends Task<Boolean> {

	private static Logger logger = Logger.getLogger(StoreDataRecordTask.class.getName());

	private DataRecord record = null;

	public StoreDataRecordTask(DataRecord record, FBase fBase) {
		super(TaskName.STORE_DATA_RECORD, fBase);
		this.record = record;
	}

	/**
	 * Put a {@link DataRecord} into the local database. Fails, if no keygroup exists for the given
	 * record.
	 * 
	 * @return true, if everything works, otherwise false
	 */
	@Override
	public Boolean executeFunctionality() {
		// get keygroup config and put into database
		try {
			fBase.connector.dataRecords_put(record);
		} catch (FBaseStorageConnectorException e) {
			logger.error(e.getMessage());
			return false;
		}
		return true;
	}

}
