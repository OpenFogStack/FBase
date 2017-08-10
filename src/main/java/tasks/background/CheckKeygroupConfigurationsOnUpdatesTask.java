package tasks.background;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.javatuples.Pair;

import control.FBase;
import exceptions.FBaseStorageConnectorException;
import model.config.KeygroupConfig;
import model.data.KeygroupID;
import tasks.Task;
import tasks.TaskManager.TaskName;
import tasks.UpdateKeygroupSubscriptionsTask;

/**
 * This background tasks checks whether any of the keygroups the executing machine is
 * responsible for have been updated without noticing. If so, it will start the
 * {@link UpdateKeygroupSubscriptionsTask} for them.
 * 
 * @author jonathanhasenburg
 *
 */
public class CheckKeygroupConfigurationsOnUpdatesTask extends Task<Boolean> {

	private static Logger logger = Logger.getLogger(CheckKeygroupConfigurationsOnUpdatesTask.class.getName());

	/**
	 * Creates a new {@link CheckKeygroupConfigurationsOnUpdatesTask}.
	 * 
	 * @param fBase
	 * @param checkInterval - the interval to check in milliseconds
	 */
	public CheckKeygroupConfigurationsOnUpdatesTask(FBase fBase, int checkInterval) {
		super(TaskName.CHECK_KEYGROUP_SUBSCRIPTIONS, fBase);
		this.checkInterval = checkInterval;
	}
	
	private int checkInterval = 10000;

	final List<KeygroupID> currentResponsibleKeygroups = new ArrayList<>();
	final Map<KeygroupConfig, Integer> currentKeygroupConfigurations = new HashMap<>();
	final Map<KeygroupConfig, Integer> savedKeygroupConfigurations = new HashMap<>();

	@Override
	public Boolean executeFunctionality() {

		while (true) {
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

				// get the keygroup configuration versions
				currentResponsibleKeygroups.forEach(k -> {
					try {
						Pair<KeygroupConfig, Integer> configs =
								fBase.configAccessHelper.keygroupConfig_get(k);
						currentKeygroupConfigurations.put(configs.getValue0(), configs.getValue1());
					} catch (FBaseStorageConnectorException e) {
						handleFBaseStorageConnectorException(e);
					}
				});

				// run update if current version and stored version differ
				// TODO I: we could add more stuff here, like unsubscribe from not existent
				// keygroup configurations, etc.
				currentKeygroupConfigurations.forEach((config, version) -> {
					if (checkIfKeygroupConfigVersionDiffers(config, version)) {
						fBase.taskmanager.runUpdateKeygroupSubscriptionsTask(config);
					}
				});

				savedKeygroupConfigurations.clear();
				savedKeygroupConfigurations.putAll(currentKeygroupConfigurations);
			} catch (FBaseStorageConnectorException e) {
				handleFBaseStorageConnectorException(e);
			}

			try {
				Thread.sleep(checkInterval);
			} catch (InterruptedException e) {
				logger.error("Background task has been interrupted!");
				e.printStackTrace();
			}
		}
	}

	private boolean checkIfKeygroupConfigVersionDiffers(KeygroupConfig config, Integer version) {
		Integer savedVersion = savedKeygroupConfigurations.get(config);
		// check whether config exists in saved map or version differs
		if (savedVersion == null || savedVersion.compareTo(version) != 0) {
			return true;
		}		
		return false;
	}

	private void handleFBaseStorageConnectorException(FBaseStorageConnectorException e) {
		// show must go on
		logger.error(e.getMessage());
		e.printStackTrace();
	}

}