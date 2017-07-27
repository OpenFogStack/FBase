package tasks;

import org.apache.log4j.Logger;

import control.FBase;
import exceptions.FBaseStorageConnectorException;
import model.config.NodeConfig;
import tasks.TaskManager.TaskName;

class UpdateNodeConfigTask extends Task<Boolean> {

	private static Logger logger = Logger.getLogger(UpdateNodeConfigTask.class.getName());

	private NodeConfig config = null;

	public UpdateNodeConfigTask(NodeConfig config, FBase fBase) {
		super(TaskName.UPDATE_NODE_CONFIG, fBase);
		this.config = config;
	}

	@Override
	public Boolean executeFunctionality() {

		// store config in database
		try {
			fBase.connector.nodeConfig_put(config.getNodeID(), config);
			logger.debug("Put node config into database");
		} catch (FBaseStorageConnectorException e) {
			logger.fatal("Could not store node configuration in node DB, nothing changed");
			return false;
		}

		return true;

	}

}
