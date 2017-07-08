package tasks;

import org.apache.log4j.Logger;

import tasks.TaskManager.TaskName;

class LogTask extends Task {

	private static Logger logger = Logger.getLogger(LogTask.class.getName());
	
	private String message = "";

	public LogTask(String message) {
		super(TaskName.LOG);
		this.message = message;
	}
	
	@Override
	public void executeFunctionality() {
		logger.info(message);
	}

	

}
