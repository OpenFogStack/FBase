package tasks;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import control.FBase;
import exceptions.FBaseException;
import exceptions.FBaseStorageConnectorException;
import model.JSONable;
import model.config.KeygroupConfig;
import model.messages.Command;
import model.messages.Envelope;
import model.messages.Message;
import tasks.TaskManager.TaskName;

/**
 * This task stores a keygroup config in the database. It will only store the configuration
 * and continue with the rest of this task, if the version differ. Furthermore, it instructs
 * the publisher to publish the given data record if a specific flag has been set.
 * 
 * This task also starts the {@link UpdateKeygroupSubscriptionsTask} for the updated keygroup.
 * 
 * @return true, if task executed
 * @return false, if version did not differ
 * 
 * @author jonathanhasenburg
 *
 */
public class UpdateKeygroupConfigTask extends Task<Boolean> {

	private static Logger logger = Logger.getLogger(UpdateKeygroupConfigTask.class.getName());

	private KeygroupConfig config = null;
	public boolean publish;

	public UpdateKeygroupConfigTask(KeygroupConfig config, FBase fBase, boolean publish) {
		super(TaskName.UPDATE_KEYGROUP_CONFIG, fBase);
		this.config = config;
		this.publish = publish;
	}

	@Override
	public Boolean executeFunctionality() throws FBaseException {

		// check version change
		KeygroupConfig oldConfig = fBase.connector.keygroupConfig_get(config.getKeygroupID());
		if (oldConfig != null && oldConfig.getVersion() >= config.getVersion()) {
			logger.warn("Version of to be put config not greater than stored config");
			return false;
		}

		// store config in database
		try {
			fBase.connector.keygroup_create(config.getKeygroupID());
		} catch (FBaseStorageConnectorException e) {
			// no problem, it just already existed
		}
		
		fBase.connector.keygroupConfig_put(config.getKeygroupID(), config);
		logger.debug("Stored configuration in database");

		try {
			fBase.taskmanager.runUpdateKeygroupSubscriptionsTask(config).get(1, TimeUnit.SECONDS);
		} catch (ExecutionException e2) {
			throw new FBaseException(e2.getCause());
		} catch (TimeoutException | InterruptedException e2) {
			e2.printStackTrace();
		}
		logger.debug("Updated keygroup subscriptions");

		if (publish) {
			// create envelope
			Message m = new Message();
			m.setCommand(Command.UPDATE_KEYGROUP_CONFIG);
			m.setContent(JSONable.toJSON(config));
			Envelope e = new Envelope(config.getKeygroupID(), m);

			// publish data
			fBase.publisher.send(e, config.getEncryptionSecret(), config.getEncryptionAlgorithm());
			logger.debug("Published updated configuration to subscribers");
		}

		return true;

	}

}
