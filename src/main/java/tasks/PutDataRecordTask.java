package tasks;

import org.apache.log4j.Logger;

import control.FBase;
import exceptions.FBaseStorageConnectorException;
import model.JSONable;
import model.config.KeygroupConfig;
import model.data.DataRecord;
import model.messages.Command;
import model.messages.Envelope;
import model.messages.Message;
import tasks.TaskManager.TaskName;

/**
 * This task takes a {@link DataRecord} and puts it into the local database using the default
 * database connector. Furthermore, it instructs the publisher to publish the given data record.
 * 
 * @author jonathanhasenburg
 */
class PutDataRecordTask extends Task<Boolean> {

	private static Logger logger = Logger.getLogger(PutDataRecordTask.class.getName());

	private DataRecord record;

	public PutDataRecordTask(DataRecord record, FBase fBase) {
		super(TaskName.PUT_DATA_RECORD, fBase);
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
			config = fBase.configAccessHelper.keygroupConfig_get(record.getKeygroupID()).getValue0();
			fBase.connector.dataRecords_put(record);
		} catch (FBaseStorageConnectorException e) {
			logger.error(e.getMessage());
			return false;
		}

		// create envelope
		Message m = new Message();
		m.setCommand(Command.PUT_DATA_RECORD);
		m.setContent(JSONable.toJSON(record));
		Envelope e = new Envelope(record.getKeygroupID(), m);

		// publish data
		fBase.publisher.send(e, config.getEncryptionSecret(),
				config.getEncryptionAlgorithm());

		return true;
	}

}
