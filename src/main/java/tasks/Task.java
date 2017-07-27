package tasks;

import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import control.FBase;
import tasks.TaskManager.TaskName;

abstract class Task<V> implements Callable<V> {

	private static Logger logger = Logger.getLogger(Task.class.getName());

	protected TaskName name = null;
	protected FBase fBase;

	/**
	 * Create a new task
	 * 
	 * @param name - the name of the task
	 * @param taskmanager - the taskmanger that keeps track of running tasks
	 */
	public Task(TaskName name, FBase fBase) {
		this.fBase = fBase;
		this.name = name;
	}

	@Override
	public V call() {
		logger.debug("Executing task " + name);
		fBase.taskmanager.registerTask(name);
		V answer = executeFunctionality();
		fBase.taskmanager.deregisterTask(name);
		logger.debug("Task " + name + " completed");
		return answer;
	}

	public abstract V executeFunctionality();

}
