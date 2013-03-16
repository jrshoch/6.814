package simpledb;

import java.util.HashMap;
import java.util.Map;

public class CountingAggregator {

  private static final Field NO_GROUPING_KEY = new StringField("No grouping", 11);

  private final boolean isGrouping;
  private final Map<Field, Integer> counts;
  private final int groupByFieldIndex;

  public CountingAggregator(int gbfield) {
    this.groupByFieldIndex = gbfield;
    this.isGrouping = (gbfield != Aggregator.NO_GROUPING);
    this.counts = new HashMap<Field, Integer>();
  }

  private Field getGroupByField(Tuple tup) {
    return isGrouping ? tup.getField(groupByFieldIndex) : NO_GROUPING_KEY;
  }

  private int getCountInternal(Field key) {
    return getValue(counts, key);
  }

  private int getValue(Map<Field, Integer> map, Field key) {
    Integer aggregateValue = map.get(key);
    return (aggregateValue == null) ? getDefaultValue() : aggregateValue.intValue();
  }

  private int getDefaultValue() {
    return 0;
  }

  public void countTuple(Tuple tup) {
    Field groupByField = getGroupByField(tup);
    counts.put(groupByField, new Integer(getCount(groupByField) + 1));
  }

  public int getCount(Field group) {
    return getCountInternal(group);
  }
  
  public int getCount() {
    return getCountInternal(NO_GROUPING_KEY);
  }
  
  public Map<Field, Integer> getCounts() {
    return new HashMap<Field, Integer>(counts);
  }
}
