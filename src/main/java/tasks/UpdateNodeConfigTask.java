package tasks;

import org.apache.log4j.Logger;

import control.FBase;
import exceptions.FBaseNamingServiceException;
import exceptions.FBaseStorageConnectorException;
import model.config.NodeConfig;
import tasks.TaskManager.TaskName;

/**
 * This task can be used to update a node configuration in the database. You can supply
 * different flags to change the performed actions.
 * 
 * After a successful update, the new config is send to the naming service if not told
 * otherwise.
 * 
 * @author jonathanhasenburg
 *
 */
public class UpdateNodeConfigTask extends Task<Boolean> {

	private static Logger logger = Logger.getLogger(UpdateNodeConfigTask.class.getName());

	public enum Flag {
		PUT("Puts the given node config in the database, replacing existing one"),
		INITIAL("Tries to add myself to an existing config from the database. "
				+ "If none existing, PUT is performed with a newly created config");

		/**
		 * Message detailing the flags purpose
		 */
		private String message;

		Flag(String message) {
			this.message = message;
		}

		public String getMessage() {
			return this.message;
		}
	};

	private NodeConfig config = null;
	private Flag flag;
	private boolean notifyNamingService = true;

	public UpdateNodeConfigTask(NodeConfig config, FBase fBase, Flag flag) {
		super(TaskName.UPDATE_NODE_CONFIG, fBase);
		this.config = config;
		this.flag = flag;
	}

	public UpdateNodeConfigTask(NodeConfig config, FBase fBase, Flag flag,
			boolean notifyNamingService) {
		super(TaskName.UPDATE_NODE_CONFIG, fBase);
		this.config = config;
		this.flag = flag;
		this.notifyNamingService = notifyNamingService;
	}

	@Override
	public Boolean executeFunctionality() {
		logger.debug("Running: " + flag.getMessage());

		if (Flag.INITIAL.equals(flag)) {
			try {
				this.config = fBase.connector.nodeConfig_get(fBase.configuration.getNodeID());
				this.config.getMachines().add(fBase.configuration.getMachineIPAddress());
			} catch (FBaseStorageConnectorException | NullPointerException e) {
				logger.error("Exception catched while trying to get config from database. "
						+ "Building new config." + e);
				this.config = fBase.configuration.buildNodeConfigBasedOnData();
			}
		}

		// put config in database
		try {
			fBase.connector.nodeConfig_put(config.getNodeID(), config);
			logger.debug("Put node config into database");
		} catch (FBaseStorageConnectorException e) {
			logger.fatal("Could not store node configuration in node DB, nothing changed");
			return false;
		}

		// send config to namingservice if desired
		if (notifyNamingService) {
			try {
				if (!fBase.namingServiceSender.sendNodeConfigCreate(this.config)) {
					fBase.namingServiceSender.sendNodeConfigUpdate(this.config);
				}
			} catch (FBaseNamingServiceException e) {
				logger.error("Could not send updated config to naming service", e);
			}
		}

		return true;

	}

}
