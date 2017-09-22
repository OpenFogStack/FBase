package tasks;

import org.apache.log4j.Logger;

import control.FBase;
import exceptions.FBaseException;
import model.JSONable;
import model.config.KeygroupConfig;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.MessageID;
import model.messages.Command;
import model.messages.Envelope;
import model.messages.Message;
import tasks.TaskManager.TaskName;

/**
 * This task deletes a {@link DataRecord} from the node database using the default database
 * connector. Furthermore, it instructs the publisher to forward the delete request if a flag
 * has been set.
 * 
 * @author jonathanhasenburg
 */
class DeleteDataRecordTask extends Task<Boolean> {

	private static Logger logger = Logger.getLogger(DeleteDataRecordTask.class.getName());

	private DataIdentifier identifier;
	public boolean publish;

	public DeleteDataRecordTask(DataIdentifier identifier, FBase fBase, boolean publish) {
		super(TaskName.DELETE_DATA_RECORD, fBase);
		this.identifier = identifier;
		this.publish = publish;
	}

	/**
	 * Delete a {@link DataRecord} from the node database and instructs the publisher to
	 * publish a delete request if {@link #publish} is true.
	 * 
	 * @return true, if everything works
	 * @throws FBaseException if no keygroup (config) for that data record exists or the
	 *             publish fails
	 */
	@Override
	public Boolean executeFunctionality() throws FBaseException {
		// get keygroup config and delete from node database
		KeygroupConfig config = null;

		config = fBase.configAccessHelper.keygroupConfig_get(identifier.getKeygroupID());
		fBase.connector.dataRecords_delete(identifier);
		logger.debug("Deleted data record " + identifier.getDataID() + " from database");

		if (publish) {
			// get next messageID
			MessageID messageID = fBase.connector.messageHistory_getNextMessageID();

			// create envelope
			Message m = new Message();
			m.setMessageID(messageID);
			m.setCommand(Command.DELETE_DATA_RECORD);
			m.setContent(JSONable.toJSON(identifier));
			Envelope e = new Envelope(identifier.getKeygroupID(), m);

			fBase.publisher.send(e, config.getEncryptionSecret(), config.getEncryptionAlgorithm());

			// store in messageHistory
			fBase.connector.messageHistory_put(messageID, identifier);
		}

		return true;
	}

}
