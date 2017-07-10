package tasks;

import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import tasks.TaskManager.TaskName;

abstract class Task<V> implements Callable<V> {

	private static Logger logger = Logger.getLogger(Task.class.getName());
	
	protected TaskName name = null;
	
	public Task(TaskName name) {
		this.name = name;
	}
	
	@Override
	public V call() {
		logger.debug("Executing task " + name);
		TaskManager.registerTask(name);
		V answer = executeFunctionality();
		TaskManager.deregisterTask(name);
		logger.debug("Task " + name + " completed");
		return answer;
	}
	
	public abstract V executeFunctionality();

	
}
