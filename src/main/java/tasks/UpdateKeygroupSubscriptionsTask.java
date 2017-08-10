package tasks;

import org.apache.log4j.Logger;

import communication.SubscriptionRegistry;
import control.FBase;
import exceptions.FBaseStorageConnectorException;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.config.ReplicaNodeConfig;
import storageconnector.AbstractDBConnector;
import tasks.TaskManager.TaskName;

/**
 * 
 * Checks what subscriptions for a given keygroup config exists and updates the subscriptions
 * using the {@link SubscriptionRegistry} if changes are necessary. Also runs
 * {@link AbstractDBConnector#keyGroupSubscriberMachines_put(model.data.KeygroupID, String)}
 * to put the own name in the database.
 * 
 * This task is usually run after a keygroup config was updated.
 * 
 * @author jonathanhasenburg
 *
 */
public class UpdateKeygroupSubscriptionsTask extends Task<Boolean> {

	private static Logger logger =
			Logger.getLogger(UpdateKeygroupSubscriptionsTask.class.getName());

	private KeygroupConfig config = null;

	public UpdateKeygroupSubscriptionsTask(KeygroupConfig config, FBase fBase) {
		super(TaskName.UPDATE_KEYGROUP_SUBSCRIPTIONS, fBase);
		this.config = config;
	}

	@Override
	public Boolean executeFunctionality() {

		if (config.getReplicaNodes() != null) {
			// TODO I: one should unsubscribe from outdated replica nodes (that no longer
			// exist)
			logger.debug("Subscribing to replica nodes (" + config.getReplicaNodes().size() + ")"
					+ " of config " + config.getKeygroupID());
			for (ReplicaNodeConfig rnConfig : config.getReplicaNodes()) {
				// don't subscribe to own machines or itself
				if (!fBase.configuration.getNodeID().equals(rnConfig.getNodeID())) {
					logger.debug("Subscribing to machines of node " + rnConfig.getNodeID().getID());
					// get node configs
					NodeConfig nodeConfig = null;
					try {
						nodeConfig = fBase.configAccessHelper.nodeConfig_get(rnConfig.getNodeID());
						if (nodeConfig != null) {
							// subscribe to all machines
							int publisherPort = nodeConfig.getPublisherPort();
							for (String machine : nodeConfig.getMachines()) {
								fBase.subscriptionRegistry.subscribeTo(machine, publisherPort,
										config.getEncryptionSecret(),
										config.getEncryptionAlgorithm(), config.getKeygroupID());
							}
						} else {
							logger.error(
									"No node config existed for " + rnConfig.getNodeID().getID());
						}

					} catch (FBaseStorageConnectorException e) {
						logger.error("Could not get node configuration from node DB for "
								+ rnConfig.getNodeID());
					}
				}
			}
		} else {
			logger.debug("No replica nodes exist config " + config.getKeygroupID());
		}

		// update keygroup subscriber machines database entry
		try {
			fBase.connector.keyGroupSubscriberMachines_put(config.getKeygroupID(),
					fBase.configuration.getMachineName());
		} catch (FBaseStorageConnectorException e) {
			e.printStackTrace();
			logger.error("Could not update keygroup subscriber machines database entry! "
					+ "Even though the subscriptions have been initialized, "
					+ "data conflicts might occur.");
		}

		return true;

	}

}
