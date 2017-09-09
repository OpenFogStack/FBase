package tasks;

import org.apache.log4j.Logger;

import control.FBase;
import exceptions.FBaseEncryptionException;
import exceptions.FBaseCommunicationException;
import exceptions.FBaseStorageConnectorException;
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
	 * publish a delete request if {@link #publish} is true. Fails, if no keygroup exists for
	 * the given record, or no related keygroup config is found.
	 * 
	 * @return true, if everything works, otherwise false
	 */
	@Override
	public Boolean executeFunctionality() {
		// get keygroup config and delete from node database
		KeygroupConfig config = null;
		try {
			config = fBase.configAccessHelper.keygroupConfig_get(identifier.getKeygroupID());
			fBase.connector.dataRecords_delete(identifier);
		} catch (FBaseStorageConnectorException | FBaseCommunicationException e) {
			logger.error(e.getMessage());
			return false;
		}

		try {
			if (publish) {
				// get next messageID
				MessageID messageID = fBase.connector.messageHistory_getNextMessageID();
				
				// create envelope
				Message m = new Message();
				m.setMessageID(messageID);
				m.setCommand(Command.DELETE_DATA_RECORD);
				m.setContent(JSONable.toJSON(identifier));
				Envelope e = new Envelope(identifier.getKeygroupID(), m);

				fBase.publisher.send(e, config.getEncryptionSecret(),
							config.getEncryptionAlgorithm());
				
				// store in messageHistory
				fBase.connector.messageHistory_put(messageID, identifier);
			}
		} catch (FBaseStorageConnectorException | FBaseEncryptionException e) {
			logger.error("Unable to publish message", e);
			return false;
		}

		return true;
	}

}
