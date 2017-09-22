package tasks.background;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import control.FBase;
import exceptions.FBaseStorageConnectorException;
import tasks.Task;
import tasks.TaskManager.TaskName;

/**
 * 
 * This background task stores heartbeats inside the node database for this machine.
 * 
 * @author jonathanhasenburg
 *
 */
public class PutHeartbeatTask extends Task<Boolean> {

	private static Logger logger = Logger.getLogger(PutHeartbeatTask.class.getName());

	static {
		logger.setLevel(Level.WARN);
	}

	/**
	 * Creates a new {@link PutHeartbeatTask}. If pulse <= 0, the default is used (2 sec)
	 * 
	 * @param fBase
	 * @param pulse - the interval to put heartbeats in milliseconds
	 */
	public PutHeartbeatTask(FBase fBase, int pulse) {
		super(TaskName.B_PUT_HEARTBEAT, fBase);
		if (pulse > 0) {
			this.pulse = pulse;
		}
	}

	private int pulse = 2000;

	@Override
	public Boolean executeFunctionality() {

		while (!Thread.currentThread().isInterrupted()) {
			logger.info("Putting heartbeat");

			try {
				fBase.connector.heartbeats_update(fBase.configuration.getMachineName(),
						fBase.configuration.getMachineIPAddress());
			} catch (FBaseStorageConnectorException e1) {
				logger.error("Could not store heartbeat in database", e1);
			}

			try {
				Thread.sleep(pulse);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				logger.error("Background task has been interrupted");
			}
		}
		logger.info("Stopping task, because Thread was interrupted");
		return true;
	}

}
