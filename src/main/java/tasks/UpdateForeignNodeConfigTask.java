package tasks;

import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import control.FBase;
import exceptions.FBaseException;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.config.ReplicaNodeConfig;
import model.data.KeygroupID;
import tasks.TaskManager.TaskName;

/**
 * This task can be used to update a foreign node configuration in the database. It will only
 * update the configuration and continue with the rest of this task, if the machines changed.
 * 
 * Also starts the {@link UpdateKeygroupSubscriptionsTask} for each keygroup the node
 * participates in
 * 
 * @return true, if task executed
 * @return false, if machines did not change
 * 
 * @author jonathanhasenburg
 *
 */
public class UpdateForeignNodeConfigTask extends Task<Boolean> {

	private static Logger logger = Logger.getLogger(UpdateForeignNodeConfigTask.class.getName());

	private NodeConfig config = null;

	public UpdateForeignNodeConfigTask(NodeConfig config, FBase fBase) {
		super(TaskName.UPDATE_NODE_CONFIG, fBase);
		this.config = config;
	}

	@Override
	public Boolean executeFunctionality() throws FBaseException {

		// check if machines changed
		NodeConfig oldConfig = fBase.connector.nodeConfig_get(config.getNodeID());
		if (oldConfig.getMachines().equals(config.getMachines())) {
			return false;
		}

		fBase.connector.nodeConfig_put(config.getNodeID(), config);
		logger.debug("Put node config into database");

		// find to be updated keygroups
		HashMap<KeygroupID, KeygroupConfig> toBeUpdatedKeygroups =
				new HashMap<KeygroupID, KeygroupConfig>();
		for (KeygroupID keygroupID : fBase.connector.keygroupConfig_list()) {
			KeygroupConfig keygroupConfig = fBase.connector.keygroupConfig_get(keygroupID);
			for (ReplicaNodeConfig repN : keygroupConfig.getReplicaNodes()) {
				if (config.getNodeID().equals(repN)) {
					toBeUpdatedKeygroups.put(keygroupID, keygroupConfig);
					break;
				}
			}
		}

		// start update keygroup subscriptions task
		toBeUpdatedKeygroups.values().parallelStream().forEach(config -> {
			try {
				fBase.taskmanager.runUpdateKeygroupSubscriptionsTask(config).get(1,
						TimeUnit.SECONDS);
			} catch (InterruptedException | ExecutionException | TimeoutException e) {
				logger.error("Could not update subscriptions for " + config.getKeygroupID(), e);
			}

		});

		return true;

	}

}
