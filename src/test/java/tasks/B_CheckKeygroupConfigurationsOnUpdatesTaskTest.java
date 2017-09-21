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

import control.FBase;
import exceptions.FBaseCommunicationException;
import exceptions.FBaseNamingServiceException;
import exceptions.FBaseStorageConnectorException;
import model.config.KeygroupConfig;
import model.config.ReplicaNodeConfig;
import model.data.KeygroupID;
import tasks.TaskManager.TaskName;

public class B_CheckKeygroupConfigurationsOnUpdatesTaskTest {

	private static Logger logger =
			Logger.getLogger(B_CheckKeygroupConfigurationsOnUpdatesTaskTest.class.getName());

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test()
			throws FBaseStorageConnectorException, InterruptedException, ExecutionException,
			TimeoutException, FBaseCommunicationException, FBaseNamingServiceException {
		logger.debug("-------Starting test-------");
		FBase fbase = new FBase(null);
		fbase.startup(false);
		fbase.taskmanager.storeHistory();
		KeygroupID id = new KeygroupID("app", "tenant", "group");
		KeygroupConfig config = new KeygroupConfig(id, null, null);
		config.setVersion(1);
		config.addReplicaNode(new ReplicaNodeConfig(fbase.configuration.getNodeID()));
		fbase.taskmanager.runUpdateKeygroupConfigTask(config, false).get(2, TimeUnit.SECONDS);
		assertEquals(
				TaskName.UPDATE_KEYGROUP_SUBSCRIPTIONS
						+ " should not have been executed so often, ",
				new Integer(1), fbase.taskmanager.getHistoricTaskNumbers()
						.get(TaskName.UPDATE_KEYGROUP_SUBSCRIPTIONS));
		Future<Boolean> task = fbase.taskmanager.runCheckKeygroupConfigurationsOnUpdatesTask(1000);
		config.setVersion(2);
		fbase.connector.keygroupConfig_put(id, config);
		Thread.sleep(2000);
		// FIXME 50: should be 2, not 3. see related task for reason
		assertEquals(TaskName.UPDATE_KEYGROUP_SUBSCRIPTIONS + " should have been twice by now, ",
				new Integer(3), fbase.taskmanager.getHistoricTaskNumbers()
						.get(TaskName.UPDATE_KEYGROUP_SUBSCRIPTIONS));
		task.cancel(true);
		fbase.tearDown();
		logger.debug("Finished test.");
	}

}
