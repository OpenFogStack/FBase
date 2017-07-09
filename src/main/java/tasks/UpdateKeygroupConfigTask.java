package tasks;

import org.apache.log4j.Logger;

import communication.SubscriptionRegistry;
import control.Mastermind;
import exceptions.FBaseStorageConnectorException;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.config.ReplicaNodeConfig;
import tasks.TaskManager.TaskName;

class UpdateKeygroupConfigTask extends Task {

	private static Logger logger = Logger.getLogger(UpdateKeygroupConfigTask.class.getName());
	
	private KeygroupConfig config = null;

	public UpdateKeygroupConfigTask(KeygroupConfig config) {
		super(TaskName.UpdateKeygroupConfig);
		this.config = config;
	}
	
	@Override
	public void executeFunctionality() {
		
		// store config in database
		try {
			Mastermind.connector.createKeygroup(config.getKeygroupID());
		} catch (FBaseStorageConnectorException e) {
			// no problem, it just already existed
		}
		try {
			Mastermind.connector.putKeygroupConfig(config.getKeygroupID(), config);
		} catch (FBaseStorageConnectorException e) {
			logger.fatal("Could not store keygroup configuration in node DB, nothing changed");
			return;
		}
		
		for (ReplicaNodeConfig rnConfig: config.getReplicaNodes()) {
			logger.debug("Subscribing to machines of node " + rnConfig.getNodeID());
			// get node configs
			NodeConfig nodeConfig = new NodeConfig(); // TODO 1: access database based on rnConfig
			
			// subscribe to all machines 
			// TODO I: we currently don't load balance the subscriptions, no failover (it is just done by the machine that runs this task)
			// TODO I: if this task is used more than once for the same keygroup config by different machines, they all subscribe to all publishers
			int publisherPort = nodeConfig.getPublisherPort();
			for (String machine: nodeConfig.getMachines()) {
				SubscriptionRegistry.subscribeTo(machine, publisherPort, config.getEncryptionSecret(), 
						config.getEncryptionAlgorithm(), config.getKeygroupID());
			}
		}
		
	}

	

}
