package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

  private static final long serialVersionUID = 1L;

  private static Field NO_GROUPING_KEY = new StringField("No grouping", 11);

  private final boolean isGrouping;
  private final Map<Field, Integer> aggregateValues;
  private final int groupByFieldIndex;
  private final Type groupByFieldType;
  private final int aggregateFieldIndex;
  private final Op aggregateOperator;
  private final CountingAggregator counter;

  /**
   * Aggregate constructor
   * 
   * @param gbfield the 0-based index of the group-by field in the tuple, or
   *          NO_GROUPING if there is no grouping
   * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or
   *          null if there is no grouping
   * @param afield the 0-based index of the aggregate field in the tuple
   * @param what the aggregation operator
   */

  public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    this.groupByFieldIndex = gbfield;
    this.isGrouping = (gbfield != NO_GROUPING);
    this.aggregateValues = new HashMap<Field, Integer>();
    this.groupByFieldType = gbfieldtype;
    this.aggregateFieldIndex = afield;
    this.aggregateOperator = what;
    this.counter = new CountingAggregator(gbfield);
  }

  private Field getGroupByField(Tuple tup) {
    return isGrouping ? tup.getField(groupByFieldIndex) : NO_GROUPING_KEY;
  }

  private int getAggregateValue(Field key) {
    return getValue(aggregateValues, key);
  }

  private int getValue(Map<Field, Integer> map, Field key) {
    Integer aggregateValue = map.get(key);
    return (aggregateValue == null) ? getDefaultValue() : aggregateValue.intValue();
  }

  private int getDefaultValue() {
    switch (aggregateOperator) {
    case AVG:
    case COUNT:
    case SUM:
      return 0;
    case MIN:
      return Integer.MAX_VALUE;
    case MAX:
      return Integer.MIN_VALUE;
    default:
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Merge a new tuple into the aggregate, grouping as indicated in the
   * constructor
   * 
   * @param tup the Tuple containing an aggregate field and a group-by field
   */
  @Override
  public void mergeTupleIntoGroup(Tuple tup) {
    Field groupByField = getGroupByField(tup);
    int previousValue = getAggregateValue(groupByField);
    counter.countTuple(tup);
    int tupleValue = ((IntField) tup.getField(aggregateFieldIndex)).getValue();
    int newValue;
    switch (aggregateOperator) {
    case AVG:
    case SUM:
      newValue = previousValue + tupleValue;
      break;
    case COUNT:
      newValue = previousValue + 1;
      break;
    case MIN:
      newValue = (previousValue < tupleValue) ? previousValue : tupleValue;
      break;
    case MAX:
      newValue = (previousValue > tupleValue) ? previousValue : tupleValue;
      break;
    default:
      throw new UnsupportedOperationException();
    }
    aggregateValues.put(groupByField, new Integer(newValue));
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
          "GROUPED BY", aggregateOperator.toString() });
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
      for (Field groupByValue : aggregateValues.keySet()) {
        int aggregateValue = getAggregateValue(groupByValue);
        if (aggregateOperator == Op.AVG) {
          aggregateValue /= counter.getCount(groupByValue);
        }
        Tuple tuple = new Tuple(tupleDesc);
        tuple.setField(0, groupByValue);
        tuple.setField(1, new IntField(aggregateValue));
        tuples.add(tuple);
      }
      return tuples;
    }
    List<Tuple> tuples = new ArrayList<Tuple>();
    Tuple tuple = new Tuple(tupleDesc);
    int aggregateValue = getAggregateValue(NO_GROUPING_KEY);
    if (aggregateOperator == Op.AVG) {
      aggregateValue /= counter.getCount();
    }
    tuple.setField(0, new IntField(aggregateValue));
    tuples.add(tuple);
    return tuples;
  }
}
