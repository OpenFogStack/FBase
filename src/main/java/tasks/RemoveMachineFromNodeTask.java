package tasks;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.javatuples.Pair;

import control.FBase;
import exceptions.FBaseException;
import exceptions.FBaseStorageConnectorException;
import model.config.KeygroupConfig;
import model.data.KeygroupID;
import tasks.TaskManager.TaskName;

class RemoveMachineFromNodeTask extends Task<Boolean> {

	private static Logger logger = Logger.getLogger(RemoveMachineFromNodeTask.class.getName());

	private String machineName = "";

	public RemoveMachineFromNodeTask(String machineName, FBase fBase) {
		super(TaskName.REMOVE_MACHINE_FROM_NODE, fBase);
		this.machineName = machineName;
	}

	@Override
	public Boolean executeFunctionality() throws FBaseException {
		logger.info("Removing machine " + machineName + " from node");

		Map<KeygroupID, Pair<String, Integer>> responsibilities =
				fBase.connector.keyGroupSubscriberMachines_listAll();

		List<KeygroupID> responsibleKeygroups = new ArrayList<>();
		boolean removalWithoutProblems = true;
		for (KeygroupID keygroupID : responsibilities.keySet()) {
			if (machineName.equals(responsibilities.get(keygroupID).getValue0())) {
				responsibleKeygroups.add(keygroupID);
				// remove from responsibility table
				try {
					fBase.connector.keyGroupSubscriberMachines_put(keygroupID, null);
				} catch (FBaseStorageConnectorException e) {
					logger.error("Could not remove responsibility of " + machineName + " for "
							+ keygroupID);
					removalWithoutProblems = false;
				}
			}
		}

		// remove from heartbeats table if all responsibilites have been removed
		// keep inside if problem so that others can try again
		if (removalWithoutProblems) {
			fBase.connector.heartbeats_remove(machineName);
		}

		// move code to separate task, that cannot executed more than once on the same machine
		fBase.taskmanager.runAnnounceUpdateOfOwnNodeConfigurationTask();

		// get keygroup configurations and run updatesubscriptions
		for (KeygroupID keygroupID : responsibleKeygroups) {
			KeygroupConfig config = fBase.connector.keygroupConfig_get(keygroupID);
			fBase.taskmanager.runUpdateKeygroupSubscriptionsTask(config);
		}

		return true;
	}

}
