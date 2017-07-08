package tasks;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

public class TaskManager {

	private static Logger logger = Logger.getLogger(TaskManager.class.getName());
	
	private static ExecutorService pool = Executors.newCachedThreadPool();

	public enum TaskName {
		LOG, SLEEP
	}

	private static Map<TaskName, Integer> runningTasks = new HashMap<TaskName, Integer>();

	public static synchronized void registerTask(TaskName name) {
		Integer value = runningTasks.get(name);
		if (value != null) {
			runningTasks.put(name, value + 1);
		} else {
			runningTasks.put(name, 1);
		}
	}

	public static synchronized void deregisterTask(TaskName name) {
		logger.debug("Deregistering task " + name);
		if (runningTasks.containsKey(name)) {
			int number = runningTasks.get(name) - 1;
			if (number > 0) {
				runningTasks.put(name, number);
			} else {
				runningTasks.remove(name);
			}
		}
	}

	public static synchronized HashMap<TaskName, Integer> getRunningTaskNumbers() {
		return new HashMap<TaskName, Integer>(runningTasks);
	}

	public static synchronized void deleteAllData() {
		runningTasks = new HashMap<TaskName, Integer>();
	}

	/*
	 * ------ Task Initiators ------
	 */

	public static Future<?> runLogTask(String message) {
		Future<?> future = pool.submit(new LogTask(message));
		return future;
	}

	public static Future<?> runSleepTask(int time) {
		Future<?> future = pool.submit(new SleepTask(time));
		return future;
	}
	
}
