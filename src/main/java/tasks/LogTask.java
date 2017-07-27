package tasks;

import org.apache.log4j.Logger;

import control.FBase;
import tasks.TaskManager.TaskName;

class LogTask extends Task<Boolean> {

	private static Logger logger = Logger.getLogger(LogTask.class.getName());

	private String message = "";

	public LogTask(String message, FBase fBase) {
		super(TaskName.LOG, fBase);
		this.message = message;
	}

	@Override
	public Boolean executeFunctionality() {
		logger.info(message);
		return true;
	}

}
