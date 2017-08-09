package tasks;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.javatuples.Pair;

import control.FBase;
import exceptions.FBaseStorageConnectorException;
import model.JSONable;
import model.config.KeygroupConfig;
import model.messages.Command;
import model.messages.Envelope;
import model.messages.Message;
import tasks.TaskManager.TaskName;

/**
 * This task stores a keygroup config in the database. Furthermore, it instructs the publisher
 * to publish the given data record if a specific flag has been set.
 * 
 * Before returning, the {@link UpdateKeygroupSubscriptionsTask} is started if one of the
 * following to conditions is met:
 * 
 * 1. the machine is the reponsible machine for the keygroups
 * 
 * 2. no machine is responsible for the keygroup yet
 * 
 * Otherwise, the subscriptions will not be updated. However, the machine responsible for the
 * subscriptions will identify the updated due to the {@link CheckKeygroupSubscriptionsTask}
 * which runs as a background process on each machine. TODO 1: Implement Task
 * 
 * @author jonathanhasenburg
 *
 */
class UpdateKeygroupConfigTask extends Task<Boolean> {

	private static Logger logger = Logger.getLogger(UpdateKeygroupConfigTask.class.getName());

	private KeygroupConfig config = null;
	public boolean publish;

	public UpdateKeygroupConfigTask(KeygroupConfig config, FBase fBase, boolean publish) {
		super(TaskName.UPDATE_KEYGROUP_CONFIG, fBase);
		this.config = config;
		this.publish = publish;
	}

	@Override
	public Boolean executeFunctionality() {

		// store config in database
		try {
			fBase.connector.keygroup_create(config.getKeygroupID());
		} catch (FBaseStorageConnectorException e) {
			// no problem, it just already existed
		}
		try {
			fBase.connector.keygroupConfig_put(config.getKeygroupID(), config);
		} catch (FBaseStorageConnectorException e) {
			logger.fatal("Could not store keygroup configuration in node DB, nothing changed");
			return false;
		}

		// check whether subscriptions need to be updated
		try {
			Pair<String, Integer> responsibleMachine = fBase.connector
					.keyGroupSubscriberMachines_listAll().get(config.getKeygroupID());
			boolean updateNeeded = false;
			if (responsibleMachine == null) {
				logger.debug("No responsible machine set yet, initiliazing "
						+ TaskManager.TaskName.UPDATE_KEYGROUP_SUBSCRIPTIONS + " task");
				updateNeeded = true;
			} else if (fBase.configuration.getMachineName()
					.equals(responsibleMachine.getValue0())) {
				logger.debug("We are the responsible machine, initiliazing "
						+ TaskManager.TaskName.UPDATE_KEYGROUP_SUBSCRIPTIONS + " task");
				updateNeeded = true;
			}

			if (updateNeeded) {
				fBase.taskmanager.runUpdateKeygroupSubscriptionsTask(config).get(1,
						TimeUnit.SECONDS);
			}
		} catch (FBaseStorageConnectorException | InterruptedException | ExecutionException
				| TimeoutException e) {
			e.printStackTrace();
			logger.fatal("Could not check whether any subscriptions need to be updated. "
					+ "However, the configuration was stored in the database.");
		}

		if (publish) {
			// create envelope
			Message m = new Message();
			m.setCommand(Command.UPDATE_KEYGROUP_CONFIG);
			m.setContent(JSONable.toJSON(config));
			Envelope e = new Envelope(config.getKeygroupID(), m);

			// publish data
			fBase.publisher.send(e, config.getEncryptionSecret(), config.getEncryptionAlgorithm());
		}
		
		return true;

	}

}
