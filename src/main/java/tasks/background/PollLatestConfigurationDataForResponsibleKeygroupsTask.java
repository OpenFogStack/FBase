package tasks.background;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.javatuples.Pair;

import control.FBase;
import exceptions.FBaseCommunicationException;
import exceptions.FBaseNamingServiceException;
import exceptions.FBaseStorageConnectorException;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.config.ReplicaNodeConfig;
import model.data.KeygroupID;
import model.data.NodeID;
import tasks.Task;
import tasks.TaskManager.TaskName;
import tasks.UpdateForeignNodeConfigTask;
import tasks.UpdateKeygroupConfigTask;

/**
 * 
 * This background tasks regularily updates the configuration data stored in the node database
 * with the data found at the naming service for all responsbile keygroups and the included
 * replica nodes, if they do not represent itself.
 * 
 * For each found {@link KeygroupConfig}, {@link UpdateKeygroupConfigTask} is executed. <br>
 * For each found {@link NodeConfig}, {@link UpdateForeignNodeConfigTask} is executed.
 * 
 * TODO T: Write test for this task
 * 
 * TODO 2: It might make sense to ask the naming service about all keygroups this node is responsbile for,
 * instead of relying on the data from the responsibility table
 * 
 * @author jonathanhasenburg
 *
 */
public class PollLatestConfigurationDataForResponsibleKeygroupsTask extends Task<Boolean> {

	private static Logger logger =
			Logger.getLogger(PollLatestConfigurationDataForResponsibleKeygroupsTask.class.getName());

	static {
		logger.setLevel(Level.WARN);
	}
	
	/**
	 * Creates a new {@link PollLatestConfigurationDataForResponsibleKeygroupsTask}. If
	 * checkInterval <= 0, the default is used (6h)
	 * 
	 * @param fBase
	 * @param checkInterval - the interval to check in milliseconds
	 */
	public PollLatestConfigurationDataForResponsibleKeygroupsTask(FBase fBase, int checkInterval) {
		super(TaskName.B_POLL_LATEST_CONFIGURATION_DATA_FOR_RESPONSIBLE_KEYGROUPS, fBase);
		if (checkInterval > 0) {
			this.checkInterval = checkInterval;
		}
	}

	private int checkInterval = 3600000; // alle 6h

	@Override
	public Boolean executeFunctionality() {

		while (!Thread.currentThread().isInterrupted()) {

			logger.debug("Polling latest configuration data");
			
			try {
				// get responsible keygroupIDs
				List<KeygroupID> keygroupIDs = new ArrayList<>();
				Map<KeygroupID, Pair<String, Integer>> responsibilities =
						fBase.connector.keyGroupSubscriberMachines_listAll();
				for (KeygroupID keygroupID : responsibilities.keySet()) {
					if (fBase.configuration.getMachineName()
							.equals(responsibilities.get(keygroupID).getValue0())) {
						keygroupIDs.add(keygroupID);
					}
				}
				logger.debug("Need to update keygroup configurations for " + keygroupIDs);

				// updated related keygroup configurations from namingservice
				List<KeygroupConfig> keygroupConfigs = new ArrayList<>();
				for (KeygroupID keygroupID : keygroupIDs) {
					try {
						KeygroupConfig keygroupConfig =
								fBase.namingServiceSender.sendKeygroupConfigRead(keygroupID);
						keygroupConfigs.add(keygroupConfig);
						fBase.taskmanager.runUpdateKeygroupConfigTask(keygroupConfig, false);
					} catch (FBaseCommunicationException | FBaseNamingServiceException e) {
						logger.error(
								"Could not get latest configuration from namingservice for keygroup"
										+ " configuration " + keygroupID,
								e);
					}
				}
				logger.debug("Updated all keygroup configurations");

				// lets wait a little for subscriptions to come down
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					logger.error("Background task has been interrupted");
				}

				// get all replica nodeIDs for keygroupIDs
				Set<NodeID> nodeIDs = new HashSet<>();
				for (KeygroupConfig config : keygroupConfigs) {
					for (ReplicaNodeConfig repNC : config.getReplicaNodes()) {
						nodeIDs.add(repNC.getNodeID());
					}
				}
				logger.debug("Need to update node configurations for " + nodeIDs);

				// update related node configurations from namingservice
				for (NodeID nodeID : nodeIDs) {
					try {
						NodeConfig nodeConfig =
								fBase.namingServiceSender.sendNodeConfigRead(nodeID);
						fBase.taskmanager.runUpdateForeignNodeConfigTask(nodeConfig);
					} catch (FBaseCommunicationException | FBaseNamingServiceException e) {
						logger.error(
								"Could not get latest configuration from namingservice for node"
										+ nodeID,
								e);
					}
				}
				logger.debug("Updated all node configurations");

			} catch (FBaseStorageConnectorException e) {
				logger.fatal("Could not get my responsibel keygroupIDs, going back to sleep", e);
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

}
