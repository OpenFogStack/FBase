package tasks;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import control.FBase;
import model.config.KeygroupConfig;
import model.config.NodeConfig;
import model.data.DataIdentifier;
import model.data.DataRecord;

public class TaskManager {

	private static Logger logger = Logger.getLogger(TaskManager.class.getName());
	private ExecutorService pool = null;
	private final AtomicInteger[] runningTasks = new AtomicInteger[TaskName.values().length];

	private FBase fBase;

	public enum TaskName {
		LOG, SLEEP, UPDATE_KEYGROUP_CONFIG, UPDATE_KEYGROUP_SUBSCRIPTIONS, PUT_DATA_RECORD,
		DELETE_DATA_RECORD, UPDATE_NODE_CONFIG
	}

	public TaskManager(FBase fBase) {
		this.fBase = fBase;
		pool = Executors.newCachedThreadPool();
		for (int i = 0; i < runningTasks.length; i++)
			runningTasks[i] = new AtomicInteger(0);
	}

	public void registerTask(TaskName name) {
		runningTasks[name.ordinal()].incrementAndGet();
	}

	public void deregisterTask(TaskName name) {
		logger.debug("Deregistering task " + name);
		runningTasks[name.ordinal()].decrementAndGet();
	}

	public Map<TaskName, Integer> getRunningTaskNumbers() {
		Map<TaskName, Integer> res = new HashMap<>();
		for (int i = 0; i < runningTasks.length; i++)
			res.put(TaskName.values()[i], runningTasks[i].get());
		return res;
	}

	public void deleteAllData() {
		for (AtomicInteger ai : runningTasks)
			ai.set(0);
	}

	/*
	 * ------ Task Initiators ------
	 */

	public Future<Boolean> runLogTask(String message) {
		Future<Boolean> future = pool.submit(new LogTask(message, fBase));
		return future;
	}

	public Future<Boolean> runSleepTask(int time) {
		Future<Boolean> future = pool.submit(new SleepTask(time, fBase));
		return future;
	}

	public Future<Boolean> runUpdateKeygroupConfigTask(KeygroupConfig config, boolean publish) {
		Future<Boolean> future = pool.submit(new UpdateKeygroupConfigTask(config, fBase, publish));
		return future;
	}
	
	public Future<Boolean> runUpdateKeygroupSubscriptionsTask(KeygroupConfig config) {
		Future<Boolean> future = pool.submit(new UpdateKeygroupSubscriptionsTask(config, fBase));
		return future;
	}

	public Future<Boolean> runUpdateNodeConfigTask(NodeConfig config) {
		Future<Boolean> future = pool.submit(new UpdateNodeConfigTask(config, fBase));
		return future;
	}

	public Future<Boolean> runPutDataRecordTask(DataRecord record, boolean publish) {
		Future<Boolean> future = pool.submit(new PutDataRecordTask(record, fBase, publish));
		return future;
	}

	public Future<Boolean> runDeleteDataRecordTask(DataIdentifier identifier, boolean publish) {
		Future<Boolean> future = pool.submit(new DeleteDataRecordTask(identifier, fBase, publish));
		return future;
	}

}
