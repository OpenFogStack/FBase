package tasks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zeromq.ZMQ;

import tasks.TaskManager.TaskName;

public class TaskManagerTest {

	private static Logger logger = Logger.getLogger(TaskManagerTest.class.getName());
	
	ZMQ.Context context = null;
	ZMQ.Socket publisher = null;

	@Before
	public void setUp() throws Exception {
		context = ZMQ.context(1);
		publisher = context.socket(ZMQ.PUB);
	}

	@After
	public void tearDown() throws Exception {
		publisher.close();
	    context.term();
		TaskManager.deleteAllData();
	}

	@Test
	public void testOne() throws InterruptedException, ExecutionException, TimeoutException {
		logger.debug("-------Starting testOne-------");
		assertNull(TaskManager.getRunningTaskNumbers().get(TaskName.SLEEP));
		Future<?> future = TaskManager.runSleepTask(500);
		Thread.sleep(50);
		assertEquals(1, TaskManager.getRunningTaskNumbers().get(TaskName.SLEEP).intValue());
		future.get(1000, TimeUnit.MILLISECONDS);
		assertNull(TaskManager.getRunningTaskNumbers().get(TaskName.SLEEP));
		logger.debug("Finished testOne.");
	}

	@Test
	public void testMany() throws InterruptedException, ExecutionException, TimeoutException {
		logger.debug("-------Starting testMany-------");
		assertNull(TaskManager.getRunningTaskNumbers().get(TaskName.SLEEP));
		int threadsC = 5;
		Future<?>[] futures = new Future<?>[5];
		for (int i = 0; i < threadsC; i++) {
			futures[i] = TaskManager.runSleepTask(500);
		}
		Thread.sleep(50);
		assertEquals(5, TaskManager.getRunningTaskNumbers().get(TaskName.SLEEP).intValue());
		for (int i = 0; i < threadsC; i++) {
			futures[i].get(1000, TimeUnit.MILLISECONDS);
		}
		assertNull(TaskManager.getRunningTaskNumbers().get(TaskName.SLEEP));
		logger.debug("Finished testMany.");
	}

}
