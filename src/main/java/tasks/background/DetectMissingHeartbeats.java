package tasks.background;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.javatuples.Pair;

import control.FBase;
import exceptions.FBaseStorageConnectorException;
import tasks.Task;
import tasks.TaskManager.TaskName;

/**
 * 
 * This background task detects missing heartbeats of other machines and removes them from the
 * node if detected.
 * 
 * @author jonathanhasenburg
 *
 */
public class DetectMissingHeartbeats extends Task<Boolean> {

	private static Logger logger = Logger.getLogger(DetectMissingHeartbeats.class.getName());

	static {
		logger.setLevel(Level.INFO);
	}

	/**
	 * Creates a new {@link DetectMissingHeartbeats}. If checkInterval <= 0, the default is used (10
	 * sec). If toleratedMissingHeartbeat <= 0, the default is used (100 sec).
	 * 
	 * @param fBase
	 * @param checkInterval - the interval to put heartbeats in milliseconds
	 * @param toleratedMissingHeartbeat - the maximum number of milliseconds tolerated for a
	 *            heartbeat to not have been updated
	 */
	public DetectMissingHeartbeats(FBase fBase, int checkInterval, int toleratedMissingHeartbeat) {
		super(TaskName.B_DETECT_MISSING_HEARTBEATS, fBase);
		if (checkInterval > 0) {
			this.checkInterval = checkInterval;
		}
		if (toleratedMissingHeartbeat > 0) {
			this.toleratedMissingHeartbeat = toleratedMissingHeartbeat;
		}
	}

	private int checkInterval = 10000;
	private int toleratedMissingHeartbeat = 10000;

	@Override
	public Boolean executeFunctionality() {

		while (!Thread.currentThread().isInterrupted()) {
			logger.debug("Looking for missing heartbeats");

			try {
				Map<String, Pair<String, Long>> heartbeats = fBase.connector.heartbeats_listAll();
				long currentTime = System.currentTimeMillis();
				heartbeats.forEach((name, info) -> {
					long timeSince = currentTime - info.getValue1();
					if (timeSince > toleratedMissingHeartbeat) {
						logger.warn("Machines " + name + " heartbeat is " + timeSince
								+ " seconds old. Removing from node");
						try {
							boolean success = fBase.taskmanager.runRemoveMachineFromNodeTask(name)
									.get(10, TimeUnit.SECONDS);
							logger.info(
									"Removal of machine " + name + " was a success?: " + success);
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
							logger.error("Background task has been interrupted");
						} catch (ExecutionException | TimeoutException e) {
							logger.error("Could not remove machine " + name + " from node ", e);
						}
					}
				});
			} catch (FBaseStorageConnectorException e1) {
				logger.error("Could not read heartbeats, going back to sleep", e1);
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
