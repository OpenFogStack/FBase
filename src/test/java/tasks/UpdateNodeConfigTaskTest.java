package tasks;

import static org.junit.Assert.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import control.FBase;
import exceptions.FBaseStorageConnectorException;
import model.JSONable;
import model.config.NodeConfig;
import storageconnector.S3DBConnector;

public class UpdateNodeConfigTaskTest {

	private static Logger logger = Logger.getLogger(UpdateNodeConfigTaskTest.class.getName());

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
	public void test() throws FBaseStorageConnectorException, InterruptedException,
			ExecutionException, TimeoutException {
		FBase fBase1 = new FBase("UpdateNodeConfigTaskTest_1.properties");
		fBase1.startup(false);

		NodeConfig config1 = fBase1.connector.nodeConfig_get(fBase1.configuration.getNodeID());
		logger.info("FBase1 NodeConfig: " + JSONable.toJSON(config1));
		assertEquals(1, config1.getMachines().size());

		FBase fBase2 = new FBase("UpdateNodeConfigTaskTest_2.properties");
		fBase2.startup(false);

		NodeConfig config2 = fBase2.connector.nodeConfig_get(fBase2.configuration.getNodeID());
		logger.info("FBase2 NodeConfig: " + JSONable.toJSON(config2));
		assertEquals(2, config2.getMachines().size());

		// cleanup
		((S3DBConnector) fBase1.connector).deleteBucket();
		fBase1.tearDown();
		fBase2.tearDown();
	}

}
