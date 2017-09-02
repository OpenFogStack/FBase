package tasks;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zeromq.ZMQ;

import control.FBase;
import tasks.TaskManager.TaskName;

public class TaskManagerTest {

	private static Logger logger = Logger.getLogger(TaskManagerTest.class.getName());

	ZMQ.Context context = null;
	ZMQ.Socket publisher = null;

	static TaskManager taskmanager = null;
	public static FBase fBase = null;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		fBase = new FBase(null);
		fBase.startup();
		taskmanager = fBase.taskmanager;
	}

	@Before
	public void setUp() throws Exception {
		context = ZMQ.context(1);
		publisher = context.socket(ZMQ.PUB);
	}

	@After
	public void tearDown() throws Exception {
		publisher.close();
		context.term();
		taskmanager.deleteAllData();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		fBase.tearDown();
	}

	@Test
	public void testOne() throws InterruptedException, ExecutionException, TimeoutException {
		logger.debug("-------Starting testOne-------");
		assertEquals(0, taskmanager.getRunningTaskNumbers().get(TaskName.SLEEP).intValue());
		Future<?> future = taskmanager.runSleepTask(500);
		Thread.sleep(50);
		assertEquals(1, taskmanager.getRunningTaskNumbers().get(TaskName.SLEEP).intValue());
		future.get(1000, TimeUnit.MILLISECONDS);
		assertEquals(0, taskmanager.getRunningTaskNumbers().get(TaskName.SLEEP).intValue());
		logger.debug("Finished testOne.");
	}

	@Test
	public void testMany() throws InterruptedException, ExecutionException, TimeoutException {
		logger.debug("-------Starting testMany-------");
		assertEquals(0, taskmanager.getRunningTaskNumbers().get(TaskName.SLEEP).intValue());
		int threadsC = 5;
		Future<?>[] futures = new Future<?>[5];
		for (int i = 0; i < threadsC; i++) {
			futures[i] = taskmanager.runSleepTask(500);
		}
		Thread.sleep(50);
		assertEquals(5, taskmanager.getRunningTaskNumbers().get(TaskName.SLEEP).intValue());
		for (int i = 0; i < threadsC; i++) {
			futures[i].get(1000, TimeUnit.MILLISECONDS);
		}
		assertEquals(0, taskmanager.getRunningTaskNumbers().get(TaskName.SLEEP).intValue());
		logger.debug("Finished testMany.");
	}

}
