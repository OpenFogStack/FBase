package tasks.background;

import java.util.List;

import org.apache.log4j.Logger;

import control.FBase;
import exceptions.FBaseCommunicationException;
import exceptions.FBaseNamingServiceException;
import exceptions.FBaseStorageConnectorException;
import model.config.ClientConfig;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.data.ClientID;
import model.data.KeygroupID;
import model.data.NodeID;
import tasks.Task;
import tasks.TaskManager.TaskName;
import tasks.UpdateKeygroupConfigTask;

/**
 * !!!!!!!!!!!!!!!!!
 * TODO REBUILD TASK
 * !!!!!!!!!!!!!!!!!
 * 
 * This background tasks regularily updates the configuration data stored in the node database
 * with the data found at the naming service. For that it does the following:
 * 
 * 1. Update all client configurations already present 
 * 
 * 2. Update all node configurations
 * present 
 * 
 * 3. Update all KeygroupConfigurations using {@link UpdateKeygroupConfigTask}.
 * 
 * By running step three, we make sure that missing client configs and node configs will be
 * loaded. In addition, step three makes sure that subscriptions are updated (also on other
 * machines later on due to {@link CheckKeygroupConfigurationsOnUpdatesTask}
 * 
 * TODO T: Add test for this background task
 * 
 * @author jonathanhasenburg
 *
 */
public class CheckNamingServiceConfigurationDataTask extends Task<Boolean> {

	private static Logger logger =
			Logger.getLogger(CheckNamingServiceConfigurationDataTask.class.getName());

	/**
	 * Creates a new {@link CheckNamingServiceConfigurationDataTask}.
	 * 
	 * @param fBase
	 * @param checkInterval - the interval to check in milliseconds
	 */
	public CheckNamingServiceConfigurationDataTask(FBase fBase, int checkInterval) {
		super(TaskName.CHECK_KEYGROUP_SUBSCRIPTIONS, fBase);
		this.checkInterval = checkInterval;
	}

	private int checkInterval = 3600000; // alle 6h

	@Override
	public Boolean executeFunctionality() {

		while (!Thread.currentThread().isInterrupted()) {

			try {
				logger.info("Updating client configurations");
				List<ClientID> existentConfigs = fBase.connector.clientConfig_list();
				for (ClientID id: existentConfigs) {
					ClientConfig newConfig;
					try {
						newConfig = fBase.namingServiceSender.sendClientConfigRead(id);
						if (newConfig != null) {
							fBase.connector.clientConfig_put(id, newConfig);
						}
					} catch (FBaseCommunicationException | FBaseNamingServiceException e) {
						logger.error(e.getMessage(), e);
					}
		
				}
				
				logger.info("Updating node configurations");
				List<NodeID> existentNodeConfigs = fBase.connector.nodeConfig_list();
				for (NodeID id: existentNodeConfigs) {
					NodeConfig newConfig;
					try {
						newConfig = fBase.namingServiceSender.sendNodeConfigRead(id);
						if (newConfig != null) {
							fBase.taskmanager.runUpdateNodeConfigTask(newConfig, Flag.PUT, false);
						}
					} catch (FBaseCommunicationException | FBaseNamingServiceException e) {
						logger.error(e.getMessage(), e);
					}
		
				}

				logger.info("Updating keygroup configurations");
				List<KeygroupID> existentKeygroupConfigs = fBase.connector.keygroupConfig_list();
				for (KeygroupID id: existentKeygroupConfigs) {
					KeygroupConfig newConfig;
					try {
						newConfig = fBase.namingServiceSender.sendKeygroupConfigRead(id);
						if (newConfig != null) {
							fBase.taskmanager.runUpdateKeygroupConfigTask(newConfig, false);
						}
					} catch (FBaseCommunicationException | FBaseNamingServiceException e) {
						logger.error(e.getMessage(), e);
					}
		
				}
				
			} catch (FBaseStorageConnectorException e1) {
				logger.error("Could not update all configurations", e1);
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
