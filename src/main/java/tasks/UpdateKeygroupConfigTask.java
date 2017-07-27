package tasks;

import org.apache.log4j.Logger;

import control.FBase;
import exceptions.FBaseStorageConnectorException;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.config.ReplicaNodeConfig;
import tasks.TaskManager.TaskName;

class UpdateKeygroupConfigTask extends Task<Boolean> {

	private static Logger logger = Logger.getLogger(UpdateKeygroupConfigTask.class.getName());

	private KeygroupConfig config = null;

	public UpdateKeygroupConfigTask(KeygroupConfig config, FBase fBase) {
		super(TaskName.UPDATE_KEYGROUP_CONFIG, fBase);
		this.config = config;
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

		if (config.getReplicaNodes() != null) {
			// TODO I: one should unsubscribe from outdated replica nodes (that no longer exist)
			logger.debug("Subscribing to replica nodes (" + config.getReplicaNodes().size() + ") "
					+ "of config " + config.getKeygroupID());
			for (ReplicaNodeConfig rnConfig : config.getReplicaNodes()) {
				logger.debug("Subscribing to machines of node " + rnConfig.getNodeID());
				// get node configs
				NodeConfig nodeConfig = null;
				try {
					nodeConfig = fBase.connector.nodeConfig_get(rnConfig.getNodeID());
					// subscribe to all machines
					// TODO I: we currently don't load balance the subscriptions, no failover (it is
					// just done by the machine that runs this task)
					// TODO I: if this task is used more than once for the same keygroup config by
					// different machines, they all subscribe to all publishers
					// TODO I: one should not subscribe to machines of the same node
					int publisherPort = nodeConfig.getPublisherPort();
					for (String machine : nodeConfig.getMachines()) {
						fBase.subscriptionRegistry.subscribeTo(machine, publisherPort,
								config.getEncryptionSecret(), config.getEncryptionAlgorithm(),
								config.getKeygroupID());
					}
				} catch (FBaseStorageConnectorException e) {
					logger.error("Could not get node configuration from node DB for "
							+ rnConfig.getNodeID());
				}

			}
		} else {
			logger.debug("No replica nodes exist config " + config.getKeygroupID());
		}

		return true;

	}

}
