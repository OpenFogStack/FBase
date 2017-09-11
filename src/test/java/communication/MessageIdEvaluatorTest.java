package communication;

import static org.junit.Assert.assertEquals;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import model.data.MessageID;
import model.data.NodeID;

/**
 * Tests the {@link MessageIdEvaluator}, but does not start the evaluateIDs task.
 * 
 * @author jonathanhasenburg
 *
 */
public class MessageIdEvaluatorTest {

	private static Logger logger = Logger.getLogger(MessageIdEvaluatorTest.class.getName());

	NodeID nodeID1 = new NodeID("nodeID1");
	NodeID nodeID2 = new NodeID("nodeID2");
	String machineName1 = "machine1";
	String machineName2 = "machine2";

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
		logger.debug("\n");
	}

	@Test
	public void test() throws InterruptedException {
		logger.debug("-------Starting test-------");
		MessageIdEvaluator evaluator = new MessageIdEvaluator(null);
		evaluator.addReceivedMessageID(new MessageID(nodeID1, machineName1, 1));
		evaluator.addReceivedMessageID(new MessageID(nodeID1, machineName1, 3));
		evaluator.addReceivedMessageID(new MessageID(nodeID1, machineName2, 2));
		evaluator.addReceivedMessageID(new MessageID(nodeID2, machineName2, 6));
		evaluator.addReceivedMessageID(new MessageID(nodeID2, machineName2, 9));

		evaluator.getMissingMessageIDs().forEach(e -> logger.debug(e.getMessageIDString()));
		assertEquals("nodeID1/machine1/2",
				evaluator.getMissingMessageIDs().get(0).getMessageIDString());
		assertEquals("nodeID2/machine2/7",
				evaluator.getMissingMessageIDs().get(1).getMessageIDString());
		assertEquals("nodeID2/machine2/8",
				evaluator.getMissingMessageIDs().get(2).getMessageIDString());

		assertEquals(3, evaluator.getMissingMessageIDs().size());

		logger.debug("Finished test.");

	}

}
