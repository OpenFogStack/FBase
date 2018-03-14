package tasks.background;

import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.javatuples.Pair;

import control.FBase;
import exceptions.FBaseException;
import exceptions.FBaseStorageConnectorException;
import model.config.KeygroupConfig;
import model.data.KeygroupID;
import tasks.Task;
import tasks.TaskManager.TaskName;

/**
 * 
 * This background task detects if a keygroup a machine created subscriptions for is not in
 * the machine's responsibility anymore
 * 
 * @author jonathanhasenburg
 *
 */
public class DetectLostResponsibility extends Task<Boolean> {

	private static Logger logger = Logger.getLogger(DetectLostResponsibility.class.getName());

	static {
		logger.setLevel(Level.INFO);
	}

	/**
	 * Creates a new {@link DetectLostResponsibility}. If checkInterval <= 0, the default is
	 * used (10 sec).
	 * 
	 * @param fBase
	 * @param checkInterval - the interval to put heartbeats in milliseconds
	 */
	public DetectLostResponsibility(FBase fBase, int checkInterval) {
		super(TaskName.B_DETECT_LOST_RESPONSIBILITY, fBase);
		if (checkInterval > 0) {
			this.checkInterval = checkInterval;
		}

	}

	private int checkInterval = 10000;

	@Override
	public Boolean executeFunctionality() {

		while (!Thread.currentThread().isInterrupted()) {
			logger.debug("Looking for lost responsibilities");

			try {
				Set<KeygroupID> subscribedKeygroups =
						fBase.subscriptionRegistry.getSubscribedKeygroups();

				Map<KeygroupID, Pair<String, Integer>> responsibilities =
						fBase.connector.keyGroupSubscriberMachines_listAll();

				for (KeygroupID keygroupID : subscribedKeygroups) {
					try {
						Pair<String, Integer> pair = responsibilities.get(keygroupID);
						if (pair == null
								|| !pair.getValue0().equals(fBase.configuration.getMachineName())) {
							logger.info("Lost responsibility for keygroup " + keygroupID);
							KeygroupConfig config = fBase.connector.keygroupConfig_get(keygroupID);
							fBase.taskmanager.runUpdateKeygroupSubscriptionsTask(config);
						}
					} catch (FBaseException e) {
						logger.error("Could not run the subscription update process for keygroup "
								+ keygroupID, e);
					}
				}

			} catch (FBaseStorageConnectorException e1) {
				logger.error(
						"Could not read data to detect lost responsibilities, going back to sleep",
						e1);
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
