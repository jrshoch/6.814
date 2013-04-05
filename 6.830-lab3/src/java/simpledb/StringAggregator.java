package simpledb;

import java.util.ArrayList;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

  private static final long serialVersionUID = 1L;

  private final boolean isGrouping;
  private final CountingAggregator counter;
  private final Type groupByFieldType;
  private final Op aggregateOperator;
  private String groupByFieldName;

  /**
   * Aggregate constructor
   * 
   * @param gbfield the 0-based index of the group-by field in the tuple, or
   *          NO_GROUPING if there is no grouping
   * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or
   *          null if there is no grouping
   * @param afield the 0-based index of the aggregate field in the tuple
   * @param what aggregation operator to use -- only supports COUNT
   * @throws IllegalArgumentException if what != COUNT
   */

  public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    if (what != Op.COUNT)
      throw new UnsupportedOperationException();
    this.isGrouping = (gbfield != NO_GROUPING);
    this.counter = new CountingAggregator(gbfield);
    this.groupByFieldType = gbfieldtype;
    this.aggregateOperator = what;
  }

  /**
   * Merge a new tuple into the aggregate, grouping as indicated in the
   * constructor
   * 
   * @param tup the Tuple containing an aggregate field and a group-by field
   */
  @Override
  public void mergeTupleIntoGroup(Tuple tup) {
    counter.countTuple(tup);
  }

  /**
   * Create a DbIterator over group aggregate results.
   * 
   * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal) if
   *         using group, or a single (aggregateVal) if no grouping. The
   *         aggregateVal is determined by the type of aggregate specified in
   *         the constructor.
   */
  @Override
  public DbIterator iterator() {
    TupleDesc resultDesc;
    if (isGrouping) {
      resultDesc = new TupleDesc(new Type[] { groupByFieldType, Type.INT_TYPE }, new String[] {
          groupByFieldName, aggregateOperator.toString() });
    } else {
      resultDesc = new TupleDesc(new Type[] { Type.INT_TYPE },
          new String[] { aggregateOperator.toString() });
    }
    Iterable<Tuple> tuples = getTuples(resultDesc);
    return new TupleIterator(resultDesc, tuples);
  }
  
  @Override
  public Iterable<Tuple> getTuples(TupleDesc tupleDesc) {
    if (isGrouping) {
      List<Tuple> tuples = new ArrayList<Tuple>();
      for (Field groupByValue : counter.getCounts().keySet()) {
        int count = counter.getCount(groupByValue);
        Tuple tuple = new Tuple(tupleDesc);
        tuple.setField(0, groupByValue);
        tuple.setField(1, new IntField(count));
        tuples.add(tuple);
      }
      return tuples;
    }
    List<Tuple> tuples = new ArrayList<Tuple>();
    Tuple tuple = new Tuple(tupleDesc);
    int count = counter.getCount();
    tuple.setField(0, new IntField(count));
    tuples.add(tuple);
    return tuples;
  }

}
