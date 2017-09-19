package tasks;

import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.javatuples.Pair;

import communication.Subscriber;
import communication.SubscriptionRegistry;
import control.FBase;
import exceptions.FBaseException;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.config.ReplicaNodeConfig;
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
	public Boolean executeFunctionality() throws FBaseException {
		logger.debug("Updating subscriptions for config " + config.getKeygroupID());

		// get old subscriber and remove them from registry
		ArrayList<Subscriber> oldSubscriber =
				fBase.subscriptionRegistry.removeSubscriberForKeygroup(config.getKeygroupID());
		int size = 0;
		if (oldSubscriber != null) {
			size = oldSubscriber.size();
		}
		logger.debug("Number of old subscriptions: " + size);

		// check whether still apart of the keygroup
		boolean partOfKeygroup = false;
		for (ReplicaNodeConfig rn : config.getReplicaNodes()) {
			if (fBase.configuration.getNodeID().equals(rn.getNodeID())) {
				partOfKeygroup = true;
			}
		}

		// check is not tombstoned
		boolean active = true; // TODO 1: set value to !config.isTombstoned();

		// check responsibility
		boolean responsible = false;
		Pair<String, Integer> keygroupResponsibility =
				fBase.connector.keyGroupSubscriberMachines_listAll().get(config.getKeygroupID());
		if (keygroupResponsibility == null || keygroupResponsibility.getValue0()
				.equals(fBase.configuration.getMachineName())) {
			responsible = true;
		}

		// if all true
		if (partOfKeygroup && active && responsible) {
			// create new subscriber via registry
			logger.debug("Creating subscriptions for " + config.getReplicaNodes().size()
					+ " replica nodes");
			for (ReplicaNodeConfig rnConfig : config.getReplicaNodes()) {
				// don't subscribe to own machines or itself
				if (!fBase.configuration.getNodeID().equals(rnConfig.getNodeID())) {
					logger.debug("Subscribing to machines of node " + rnConfig.getNodeID().getID());
					// get node configs
					NodeConfig nodeConfig = null;
					nodeConfig = fBase.configAccessHelper.nodeConfig_get(rnConfig.getNodeID());
					if (nodeConfig != null && nodeConfig.getPublisherPort() != null
							&& nodeConfig.getMachines() != null) {
						// subscribe to all machines
						int publisherPort = nodeConfig.getPublisherPort();
						for (String machine : nodeConfig.getMachines()) {
							fBase.subscriptionRegistry.subscribeTo("tcp://" + machine,
									publisherPort, config.getEncryptionSecret(),
									config.getEncryptionAlgorithm(), config.getKeygroupID());
						}
					} else {
						logger.error("No node config existed for " + rnConfig.getNodeID().getID());
					}

				}
			}
			// update keygroup subscriber machines database entry
			fBase.connector.keyGroupSubscriberMachines_put(config.getKeygroupID(),
					fBase.configuration.getMachineName());

			logger.debug("Number of new subscriptions: "
					+ fBase.subscriptionRegistry.getNumberOfActiveSubscriptions(config.getID()));
		}

		// stop old subscriber if existent
		if (oldSubscriber != null) {
			oldSubscriber.forEach(s -> s.stopReception());
			logger.debug("Stopped old subscribers");
		}

		return true;

	}

}
