package tasks;

import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import tasks.TaskManager.TaskName;

abstract class Task<V> implements Callable<V> {

	private static Logger logger = Logger.getLogger(Task.class.getName());
	
	protected TaskName name = null;
	private TaskManager taskmanager = null;
	
	/**
	 * Create a new task
	 * @param name - the name of the task
	 * @param taskmanager - the taskmanger that keeps track of running tasks
	 */
	public Task(TaskName name, TaskManager taskmanager) {
		this.name = name;
		this.taskmanager = taskmanager;
	}
	
	@Override
	public V call() {
		logger.debug("Executing task " + name);
		taskmanager.registerTask(name);
		V answer = executeFunctionality();
		taskmanager.deregisterTask(name);
		logger.debug("Task " + name + " completed");
		return answer;
	}
	
	public abstract V executeFunctionality();

	
}
