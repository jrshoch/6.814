package simpledb;

import static org.junit.Assert.assertEquals;

import java.util.NoSuchElementException;

import junit.framework.JUnit4TestAdapter;

import org.junit.Before;
import org.junit.Test;

import simpledb.systemtest.SimpleDbTestBase;

public class StringAggregatorTest extends SimpleDbTestBase {

  int width1 = 2;
  DbIterator scan1;
  int[][] count = null;

  /**
   * Initialize each unit test
   */
  @Before
  public void createTupleList() throws Exception {
    this.scan1 = TestUtil.createTupleList(width1, new Object[] { new Integer(1), "a",
        new Integer(1), "b", new Integer(1), "c", new Integer(3), "d", new Integer(3), "e",
        new Integer(3), "f", new Integer(5), "g" });

    // verify how the results progress after a few merges
    this.count = new int[][] { { 1, 1 }, { 1, 2 }, { 1, 3 }, { 1, 3, 3, 1 } };

  }

  /**
   * Test String.mergeTupleIntoGroup() and iterator() over a COUNT
   */
  @Test
  public void mergeCount() throws Exception {
    scan1.open();
    StringAggregator agg = new StringAggregator(0, Type.INT_TYPE, 1, Aggregator.Op.COUNT);

    for (int[] step : count) {
      agg.mergeTupleIntoGroup(scan1.next());
      DbIterator it = agg.iterator();
      it.open();
      TestUtil.matchAllTuples(TestUtil.createTupleList(width1, step), it);
    }
  }

  /**
   * Test StringAggregator.iterator() for DbIterator behaviour
   */
  @Test
  public void testIterator() throws Exception {
    // first, populate the aggregator via sum over scan1
    scan1.open();
    StringAggregator agg = new StringAggregator(0, Type.INT_TYPE, 1, Aggregator.Op.COUNT);
    try {
      while (true)
        agg.mergeTupleIntoGroup(scan1.next());
    } catch (NoSuchElementException e) {
      // explicitly ignored
    }

    DbIterator it = agg.iterator();
    it.open();

    // verify it has three elements
    int elementCount = 0;
    try {
      while (true) {
        it.next();
        elementCount++;
      }
    } catch (NoSuchElementException e) {
      // explicitly ignored
    }
    assertEquals(3, elementCount);

    // rewind and try again
    it.rewind();
    elementCount = 0;
    try {
      while (true) {
        it.next();
        elementCount++;
      }
    } catch (NoSuchElementException e) {
      // explicitly ignored
    }
    assertEquals(3, elementCount);

    // close it and check that we don't get anything
    it.close();
    try {
      it.next();
      throw new Exception("StringAggreator iterator yielded tuple after close");
    } catch (Exception e) {
      // explicitly ignored
    }
  }

  /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(StringAggregatorTest.class);
  }
}
