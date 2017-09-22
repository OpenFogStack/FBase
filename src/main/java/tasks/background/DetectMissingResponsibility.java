package tasks.background;

import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.javatuples.Pair;

import control.FBase;
import exceptions.FBaseException;
import exceptions.FBaseStorageConnectorException;
import model.data.KeygroupID;
import tasks.Task;
import tasks.TaskManager.TaskName;

/**
 * 
 * This background task detects keygroups which do not have a responsiblity assigned
 * 
 * @author jonathanhasenburg
 *
 */
public class DetectMissingResponsibility extends Task<Boolean> {

	private static Logger logger = Logger.getLogger(DetectMissingResponsibility.class.getName());

	static {
		logger.setLevel(Level.INFO);
	}

	/**
	 * Creates a new {@link DetectMissingResponsibility}. If checkInterval <= 0, the default
	 * is used (10 sec).
	 * 
	 * @param fBase
	 * @param checkInterval - the interval to put heartbeats in milliseconds
	 */
	public DetectMissingResponsibility(FBase fBase, int checkInterval) {
		super(TaskName.B_DETECT_MISSING_RESPONSIBILITY, fBase);
		if (checkInterval > 0) {
			this.checkInterval = checkInterval;
		}

	}

	private int checkInterval = 10000;

	@Override
	public Boolean executeFunctionality() {

		while (!Thread.currentThread().isInterrupted()) {
			logger.info("Looking for missing responsibilities");

			try {
				Map<KeygroupID, Pair<String, Integer>> responsibilities =
						fBase.connector.keyGroupSubscriberMachines_listAll();

				for (KeygroupID keygroupID : responsibilities.keySet()) {
					if (responsibilities.get(keygroupID).getValue0() == null) {
						try {
							fBase.taskmanager.runUpdateKeygroupSubscriptionsTask(
									fBase.connector.keygroupConfig_get(keygroupID));
						} catch (FBaseException e) {
							logger.error(
									"Could not update responsibilites for keygroup " + keygroupID);
						}
					}
				}

			} catch (FBaseStorageConnectorException e1) {
				logger.error("Could not read responsibilities, going back to sleep", e1);
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
