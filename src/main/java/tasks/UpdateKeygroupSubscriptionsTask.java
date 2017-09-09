package tasks;

import java.util.ArrayList;

import org.apache.log4j.Logger;

import communication.Subscriber;
import communication.SubscriptionRegistry;
import control.FBase;
import exceptions.FBaseCommunicationException;
import exceptions.FBaseStorageConnectorException;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import storageconnector.AbstractDBConnector;
import tasks.TaskManager.TaskName;

/**
 * 
 * Uses the {@link SubscriptionRegistry} to stop all current subscriptions for a keygroup and
 * replace them with new ones. Adds the new subscriptions, before the old ones are removed.
 * Also runs
 * {@link AbstractDBConnector#keyGroupSubscriberMachines_put(model.data.KeygroupID, String)}
 * to put the own name in the database.
 * 
 * This task is usually run after a keygroup config was updated, e.g., through
 * {@link UpdateKeygroupConfigTask}, or when a machine takes over a subscription from another
 * machine.
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
		// TODO 1: handle config is tombstoned
		
		logger.debug("Updating subscriptions for config " + config.getKeygroupID());

		if (config.getReplicaNodes() != null) {

			// get old subscriber and remove them from registry
			ArrayList<Subscriber> oldSubscriber =
					fBase.subscriptionRegistry.removeSubscriberForKeygroup(config.getKeygroupID());
			int size = 0;
			if (oldSubscriber != null) {
				size = oldSubscriber.size();
			}
			logger.debug("Number of old subscriptions: " + size);

			// create new subscriber via registry
			logger.debug("Creating subscriptions for " + config.getReplicaNodes().size()
					+ " replica nodes");
			config.getReplicaNodes().forEach(rnConfig -> {
				// don't subscribe to own machines or itself
				if (!fBase.configuration.getNodeID().equals(rnConfig.getNodeID())) {
					logger.debug("Subscribing to machines of node " + rnConfig.getNodeID().getID());
					// get node configs
					NodeConfig nodeConfig = null;
					try {
						try {
							nodeConfig = fBase.configAccessHelper.nodeConfig_get(rnConfig.getNodeID());
						} catch (FBaseCommunicationException e) {
							logger.error("No config locally, but could not connect to naming "
									+ "service, but a config might exist there");
						}
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
								+ rnConfig.getNodeID() + ", so no subscription could be created");
					}
				}
			});
			logger.debug("Number of new subscriptions: "
					+ fBase.subscriptionRegistry.getNumberOfActiveSubscriptions(config.getID()));

			// stop old subscriber if existent
			if (oldSubscriber != null) {
				oldSubscriber.forEach(s -> s.stopReception());
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
