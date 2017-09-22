package scenario;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * In total, 8 events that trigger the subscription update process exist. This test only
 * starts one nodes, so it is tested whether the subscriptions are as expected at the end.
 * 
 * 
 * TODO T: Add missing subscription update process tests
 * 
 * [1] Missing heartbeats detected <br>
 * [2] No responsibility detected <br>
 * [3] Lost responsibility detected <br>
 * [4] Client updates/deletes keygroup via node <br>
 * [5] Not interpretable message <br>
 * [6] Periodic configuration update <br>
 * [7] Recognize foreign keygroup update <br>
 * Subscriber receives configuration update <br>
 * 
 * @author jonathanhasenburg
 *
 */
public class SubscriptionUpdateProcessScenario {

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
	public void testMissingHeartbeatDetected() {
		fail("Not yet implemented");
	}

}
