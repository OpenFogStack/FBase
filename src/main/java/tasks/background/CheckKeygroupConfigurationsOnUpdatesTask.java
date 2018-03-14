package tasks.background;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.javatuples.Pair;

import control.FBase;
import exceptions.FBaseCommunicationException;
import exceptions.FBaseException;
import exceptions.FBaseNamingServiceException;
import exceptions.FBaseStorageConnectorException;
import model.config.KeygroupConfig;
import model.data.KeygroupID;
import tasks.Task;
import tasks.TaskManager.TaskName;
import tasks.UpdateKeygroupConfigTask;
import tasks.UpdateKeygroupSubscriptionsTask;

/**
 * 
 * This background tasks checks whether any of the keygroups the executing machine is
 * responsible for have been updated without noticing. If so, it will start the
 * {@link UpdateKeygroupSubscriptionsTask} for them.
 * 
 * An example case in which this functionality is required, is when a client updates a
 * keygroup configuration via another machine but the responsible one. In this case, the not
 * responsible machine will receive the updated configuration.
 * 
 * FIXME 50: savedKeygroupConfigurations must be updated by {@link UpdateKeygroupConfigTask},
 * otherwise this task will update the subscriptions even though they have been updated by
 * {@link UpdateKeygroupConfigTask} already.
 * 
 * @author jonathanhasenburg
 *
 */
public class CheckKeygroupConfigurationsOnUpdatesTask extends Task<Boolean> {

	private static Logger logger =
			Logger.getLogger(CheckKeygroupConfigurationsOnUpdatesTask.class.getName());

	static {
		logger.setLevel(Level.INFO);
	}
	
	/**
	 * Creates a new {@link CheckKeygroupConfigurationsOnUpdatesTask}. If checkInterval <= 0,
	 * the default is used (10 sec)
	 * 
	 * @param fBase
	 * @param checkInterval - the interval to check in milliseconds
	 */
	public CheckKeygroupConfigurationsOnUpdatesTask(FBase fBase, int checkInterval) {
		super(TaskName.B_CHECK_KEYGROUP_CONFIGURATIONS_ON_UPDATES, fBase);
		if (checkInterval > 0) {
			this.checkInterval = checkInterval;
		}
	}

	private int checkInterval = 10000;

	final List<KeygroupID> currentResponsibleKeygroups = new ArrayList<>();
	final Map<KeygroupID, KeygroupConfig> currentKeygroupConfigurations = new HashMap<>();
	final Map<KeygroupID, KeygroupConfig> savedKeygroupConfigurations = new HashMap<>();

	@Override
	public Boolean executeFunctionality() {

		while (!Thread.currentThread().isInterrupted()) {
			logger.debug("Checking keygroup configurations on updates");
			
			currentResponsibleKeygroups.clear();
			currentKeygroupConfigurations.clear();
			try {
				// get all keygroups I am currently responsible for
				Map<KeygroupID, Pair<String, Integer>> keygroupSubscriberMachines =
						fBase.connector.keyGroupSubscriberMachines_listAll();
				keygroupSubscriberMachines.forEach((k, v) -> {
					if (fBase.configuration.getMachineName().equals(v.getValue0())) {
						currentResponsibleKeygroups.add(k);
					}
				});
				logger.debug("Number of currently responsible keygroups / all keygroups: "
						+ currentResponsibleKeygroups.size() + " / "
						+ keygroupSubscriberMachines.size());

				// get the keygroup configurations for the responsible keygroups
				currentResponsibleKeygroups.forEach(k -> {
					try {
						KeygroupConfig config = fBase.configAccessHelper.keygroupConfig_get(k);
						currentKeygroupConfigurations.put(k, config);
					} catch (FBaseStorageConnectorException | FBaseCommunicationException
							| FBaseNamingServiceException e) {
						handleFBaseException(e);
					}
				});

				// run update if current version and stored version differ
				currentKeygroupConfigurations.forEach((keygroupID, config) -> {
					if (checkIfKeygroupConfigVersionDiffers(keygroupID, config.getVersion())) {
						fBase.taskmanager.runUpdateKeygroupSubscriptionsTask(config);
					}
				});

				savedKeygroupConfigurations.clear();
				savedKeygroupConfigurations.putAll(currentKeygroupConfigurations);
			} catch (FBaseStorageConnectorException e) {
				handleFBaseException(e);
			}

			try {
				Thread.sleep(checkInterval);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				logger.error("Background task has been interrupted");
			}
		}
		logger.info("Stopping task, because Thread was interrupted");
		return true;
	}

	private boolean checkIfKeygroupConfigVersionDiffers(KeygroupID keygroupID, Integer version) {
		Integer savedVersion = Optional.ofNullable(savedKeygroupConfigurations.get(keygroupID))
				.map(KeygroupConfig::getVersion).orElse(null);
		// check whether config exists in saved map or version increased
		if (savedVersion == null || savedVersion.compareTo(version) < 0) {
			logger.debug("Version of config " + keygroupID + " increased from " + savedVersion
					+ " to " + version);
			return true;
		}
		return false;
	}

	private void handleFBaseException(FBaseException e) {
		// show must go on
		logger.error(e.getMessage());
		e.printStackTrace();
	}

}
