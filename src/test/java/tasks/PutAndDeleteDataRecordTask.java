package tasks;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import scenario.TwoNodeScenario;

/**
 * Test {@link PutDataRecordTask} and {@link DeleteDataRecordTask}.
 * 
 * Does not setup a second node to see whether publishing works, because this already tested
 * in {@link TwoNodeScenario#testUpdateDataRecord()}
 * 
 * This test is especially important for the message history testing.
 * 
 * @author jonathanhasenburg
 *
 */
public class PutAndDeleteDataRecordTask {

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
	public void test() {
		// TODO 1: Implement me!
		fail("Not yet implemented");
	}

}
