package tasks;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import control.FBase;
import exceptions.FBaseCommunicationException;
import exceptions.FBaseNamingServiceException;
import model.config.KeygroupConfig;
import model.data.KeygroupID;
import model.messages.Envelope;
import tasks.TaskManager.TaskName;

/**
 * This task tries to process a message which could not be interpreted with the encryption
 * data available locally. This is usually the case, if the secret of a configuration was
 * updated. Thus, the task polls the most recent configuration of the keygroup from the naming
 * service and initializes the subscription update process
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
		logger.debug("Trying to process envelope with keygroup id " + envelope.getKeygroupID());

		KeygroupID keygroupID = envelope.getKeygroupID();
		if (keygroupID == null) {
			// Message is not parseable
			logger.warn("The received message does not have a valid keygroup id " + keygroupID);
			return false;
		}

		KeygroupConfig config;
		try {
			config = fBase.namingServiceSender.sendKeygroupConfigRead(envelope.getKeygroupID());
		} catch (FBaseCommunicationException | FBaseNamingServiceException e1) {
			// cannot connect to naming service
			logger.error("Cannot connect to naming service");
			return false;
		}

		try {
			fBase.taskmanager.runUpdateKeygroupConfigTask(config, false).get(3, TimeUnit.SECONDS);
		} catch (ExecutionException e) {
			logger.error("Could not update keygroup config" + e.getMessage());
		} catch (TimeoutException | InterruptedException e) {
			logger.error(e);
		}
		logger.debug("Updated subscriptions with new config");

		return true;
	}

}
