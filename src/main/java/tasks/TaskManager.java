package tasks;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.data.DataRecord;

public class TaskManager {

	private static Logger logger = Logger.getLogger(TaskManager.class.getName());
	private ExecutorService pool = null;
	private Map<TaskName, Integer> runningTasks = null;

	public enum TaskName {
		LOG, SLEEP, UPDATE_KEYGROUP_CONFIG, PUT_DATA_RECORD, STORE_DATA_RECORD, UPDATE_NODE_CONFIG
	}

	public TaskManager() {
		 pool = Executors.newCachedThreadPool();
		 runningTasks = new HashMap<TaskName, Integer>();
	}
	
	public synchronized void registerTask(TaskName name) {
		Integer value = runningTasks.get(name);
		if (value != null) {
			runningTasks.put(name, value + 1);
		} else {
			runningTasks.put(name, 1);
		}
	}

	public synchronized void deregisterTask(TaskName name) {
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

	public synchronized HashMap<TaskName, Integer> getRunningTaskNumbers() {
		return new HashMap<TaskName, Integer>(runningTasks);
	}

	public synchronized void deleteAllData() {
		runningTasks = new HashMap<TaskName, Integer>();
	}

	/*
	 * ------ Task Initiators ------
	 */

	public Future<Boolean> runLogTask(String message) {
		Future<Boolean> future = pool.submit(new LogTask(message, this));
		return future;
	}

	public Future<Boolean> runSleepTask(int time) {
		Future<Boolean> future = pool.submit(new SleepTask(time, this));
		return future;
	}
	
	public Future<Boolean> runUpdateKeygroupConfigTask(KeygroupConfig config) {
		Future<Boolean> future = pool.submit(new UpdateKeygroupConfigTask(config, this));
		return future;
	}
	
	public Future<Boolean> runUpdateNodeConfigTask(NodeConfig config) {
		Future<Boolean> future = pool.submit(new UpdateNodeConfigTask(config, this));
		return future;
	}
	
	public Future<Boolean> runPutDataRecordTask(DataRecord record) {
		Future<Boolean> future = pool.submit(new PutDataRecordTask(record, this));
		return future;
	}
	
	public Future<Boolean> runStoreDataRecordTask(DataRecord record) {
		Future<Boolean> future = pool.submit(new StoreDataRecordTask(record, this));
		return future;
	}
	
}
