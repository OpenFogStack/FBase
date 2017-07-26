package tasks;

import org.apache.log4j.Logger;

import control.FBase;
import exceptions.FBaseStorageConnectorException;
import model.JSONable;
import model.config.KeygroupConfig;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.messages.datarecords.Envelope;
import model.messages.datarecords.Message;
import tasks.TaskManager.TaskName;

/**
 * This task deletes a {@link DataRecord} from the node database using the default database connector.
 * Furthermore, it instructs the publisher to forward the delete request.
 * 
 * @author jonathanhasenburg
 */
class DeleteDataRecordTask extends Task<Boolean> {

	private static Logger logger = Logger.getLogger(DeleteDataRecordTask.class.getName());
	
	private DataIdentifier identifier;

	public DeleteDataRecordTask(DataIdentifier identifier, FBase fBase) {
		super(TaskName.DELETE_DATA_RECORD, fBase);
		this.identifier = identifier;
	}
	
	/**
	 * Delete a {@link DataRecord} from the node database and instruct the to publish a delete request.
	 * Fails, if no keygroup exists for the given record, or no related keygroup config is found.
	 * 
	 * @return true, if everything works, otherwise false
	 */
	@Override
	public Boolean executeFunctionality() {
		// get keygroup config and delete from node database
		KeygroupConfig config = null;	
		try {
			config = fBase.connector.keygroupConfig_get(identifier.getKeygroupID());
			fBase.connector.dataRecords_delete(identifier);
		} catch (FBaseStorageConnectorException e) {
			logger.error(e.getMessage());
			return false;
		}
		
		// create envelope
		Message m = new Message();
		m.setTextualResponse("DELETE");
		m.setContent(JSONable.toJSON(identifier));
		Envelope e = new Envelope(identifier.getKeygroupID(), m);
		
		// publish data
		fBase.publisher.sendKeygroupIDData(e, config.getEncryptionSecret(), config.getEncryptionAlgorithm());
		
		return true;
	}

	

}
