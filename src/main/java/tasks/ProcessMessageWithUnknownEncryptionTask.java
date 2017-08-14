package tasks;

import org.apache.log4j.Logger;

import communication.SubscriptionRegistry;
import control.FBase;
import exceptions.FBaseEncryptionException;
import model.JSONable;
import model.config.KeygroupConfig;
import model.data.DataIdentifier;
import model.data.DataRecord;
import model.data.KeygroupID;
import model.messages.Command;
import model.messages.Envelope;
import tasks.TaskManager.TaskName;

/**
 * This task tries to process a message which could not be interpreted with the encryption
 * data available locally. This is usually the case, if the secret of a configuration was
 * updated. Thus, the task polls the most recent configuration of the keygroup from the naming
 * service.
 * 
 * If the node was removed from the keygroup, this task will detect it, remove the config data
 * stored in the node database and call
 * {@link SubscriptionRegistry#unsubscribeFromKeygroup(model.data.KeygroupID)}
 * 
 * @author jonathanhasenburg
 *
 */
class ProcessMessageWithUnknownEncryptionTask extends Task<Boolean> {

	private static Logger logger =
			Logger.getLogger(ProcessMessageWithUnknownEncryptionTask.class.getName());

	public ProcessMessageWithUnknownEncryptionTask(FBase fBase, Envelope envelope) {
		super(TaskName.PROCESS_MESSAGE_WITH_UNKNOWN_ENCRYPTION, fBase);
		this.envelope = envelope;
	}

	private Envelope envelope = null;

	@Override
	public Boolean executeFunctionality() {
		logger.debug("Trying to process envelope with id " + envelope.getKeygroupID());

		KeygroupID keygroupID = envelope.getKeygroupID();
		if (keygroupID == null) {
			// Message is not parseable
			return false;
		}

		KeygroupConfig config =
				fBase.namingServiceSender.sendKeygroupConfigRead(envelope.getKeygroupID());

		if (config == null) {
			// cannot connect to naming service
			return false;
		}

		// TODO NS: process config was deleted (don't know how NS anwser looks like yet)

		// I am removed from config
		if (config.getEncryptionSecret() == null) {
			logger.debug("I was removed from the configuration");
			fBase.subscriptionRegistry.unsubscribeFromKeygroup(envelope.getKeygroupID());
			return true;
		}

		fBase.taskmanager.runUpdateKeygroupConfigTask(config, false);
		try {
			envelope.getMessage().decryptFields(config.getEncryptionSecret(),
					config.getEncryptionAlgorithm());
			if (Command.PUT_DATA_RECORD.equals(envelope.getMessage().getCommand())) {
				DataRecord update =
						JSONable.fromJSON(envelope.getMessage().getContent(), DataRecord.class);
				fBase.taskmanager.runPutDataRecordTask(update, false);
			} else if (Command.DELETE_DATA_RECORD.equals(envelope.getMessage().getCommand())) {
				DataIdentifier identifier =
						JSONable.fromJSON(envelope.getMessage().getContent(), DataIdentifier.class);
				fBase.taskmanager.runDeleteDataRecordTask(identifier, false);
			}
		} catch (FBaseEncryptionException e) {
			logger.error(
					"Could not decrypt fields with new naming service data, " + e.getMessage());
		}

		return true;
	}

}
