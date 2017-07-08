package tasks;

import org.apache.log4j.Logger;

import tasks.TaskManager.TaskName;

abstract class Task implements Runnable {

	private static Logger logger = Logger.getLogger(Task.class.getName());
	
	protected TaskName name = null;
	
	public Task(TaskName name) {
		this.name = name;
	}
	
	@Override
	public void run() {
		logger.debug("Executing task " + name);
		TaskManager.registerTask(name);
		executeFunctionality();
		TaskManager.deregisterTask(name);
		logger.debug("Task " + name + " completed");
	}
	
	public abstract void executeFunctionality();

	
}
