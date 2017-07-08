package tasks;

import org.apache.log4j.Logger;

import tasks.TaskManager.TaskName;

/**
 * A task that sleeps a given amount of time.
 * @author jonathanhasenburg
 *
 */
class SleepTask extends Task {

	private static Logger logger = Logger.getLogger(SleepTask.class.getName());
	
	private int time = -1;

	public SleepTask(int time) {
		super(TaskName.SLEEP);
		this.time = time;
	}
	
	@Override
	public void executeFunctionality() {
		if (time > 0) {
			try {
				Thread.sleep(time);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} else {
			logger.warn("Sleep time must be greater than 0, but is " + time + ".");
		}
	}
	

}
