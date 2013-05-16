package simpledb;

import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

  private static final long serialVersionUID = 1L;

  private DbIterator child;
  private boolean open;
  private final boolean isGrouping;
  private final int aggregateFieldIndex;
  private final int groupByFieldIndex;
  private final Aggregator.Op aggregateOperator;
  private Aggregator aggregator;
  private DbIterator aggregateResultsIterator;

  /**
   * Constructor.
   * 
   * Implementation hint: depending on the type of afield, you will want to
   * construct an {@link IntAggregator} or {@link StringAggregator} to help you
   * with your implementation of readNext().
   * 
   * 
   * @param child The DbIterator that is feeding us tuples.
   * @param afield The column over which we are computing an aggregate.
   * @param gfield The column over which we are grouping the result, or -1 if
   *          there is no grouping
   * @param aop The aggregation operator to use
   */
  public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
    this.child = child;
    this.open = false;
    this.isGrouping = gfield != Aggregator.NO_GROUPING;
    this.aggregateFieldIndex = afield;
    this.groupByFieldIndex = gfield;
    this.aggregateOperator = aop;
    resetAggregator();
  }

  private void resetAggregator() {
    Type groupByFieldType = isGrouping ? child.getTupleDesc().getFieldType(groupByFieldIndex) : null;
    switch (child.getTupleDesc().getFieldType(aggregateFieldIndex)) {
    case INT_TYPE:
      this.aggregator = new IntegerAggregator(groupByFieldIndex, groupByFieldType,
          aggregateFieldIndex, aggregateOperator);
      break;
    case STRING_TYPE:
      this.aggregator = new StringAggregator(groupByFieldIndex, groupByFieldType,
          aggregateFieldIndex, aggregateOperator);
      break;
    default:
      throw new UnsupportedOperationException();
    }
  }

  /**
   * @return If this aggregate is accompanied by a groupby, return the groupby
   *         field index in the <b>INPUT</b> tuples. If not, return
   *         {@link simpledb.Aggregator#NO_GROUPING}
   * */
  public int groupField() {
    return groupByFieldIndex;
  }

  /**
   * @return If this aggregate is accompanied by a group by, return the name of
   *         the groupby field in the <b>OUTPUT</b> tuples If not, return null;
   * */
  public String groupFieldName() {
    return isGrouping ? getTupleDesc().getFieldName(groupByFieldIndex) : null;
  }

  /**
   * @return the aggregate field
   * */
  public int aggregateField() {
    return aggregateFieldIndex;
  }

  /**
   * @return return the name of the aggregate field in the <b>OUTPUT</b> tuples
   * */
  public String aggregateFieldName() {
    return child.getTupleDesc().getFieldName(aggregateFieldIndex);
  }

  /**
   * @return return the aggregate operator
   * */
  public Aggregator.Op aggregateOp() {
    return aggregateOperator;
  }

  public static String nameOfAggregatorOp(Aggregator.Op aop) {
    return aop.toString();
  }

  @Override
  public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
    this.open = true;
    super.open();
    child.open();
    while (child.hasNext()) {
      aggregator.mergeTupleIntoGroup(child.next());
    }
    child.close();
    TupleDesc tupleDesc = getTupleDesc();
    aggregateResultsIterator = new TupleIterator(tupleDesc, aggregator.getTuples(tupleDesc));
    aggregateResultsIterator.open();
  }

  /**
   * Returns the next tuple. If there is a group by field, then the first field
   * is the field by which we are grouping, and the second field is the result
   * of computing the aggregate, If there is no group by field, then the result
   * tuple should contain one field representing the result of the aggregate.
   * Should return null if there are no more tuples.
   */
  @Override
  protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    if (aggregateResultsIterator.hasNext()) {
      return aggregateResultsIterator.next();
    }
    return null;
  }

  @Override
  public void rewind() throws DbException, TransactionAbortedException {
    if (!open)
      throw new IllegalStateException();
    aggregateResultsIterator.rewind();
  }

  /**
   * Returns the TupleDesc of this Aggregate. If there is no group by field,
   * this will have one field - the aggregate column. If there is a group by
   * field, the first field will be the group by field, and the second will be
   * the aggregate value column.
   * 
   * The name of an aggregate column should be informative. For example:
   * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
   * given in the constructor, and child_td is the TupleDesc of the child
   * iterator.
   */
  @Override
  public TupleDesc getTupleDesc() {
    if (isGrouping) {
      return new TupleDesc(new Type[] { child.getTupleDesc().getFieldType(groupByFieldIndex),
          Type.INT_TYPE }, new String[] {
          child.getTupleDesc().getFieldName(groupByFieldIndex),
          aggregateOperator.toString() + " ("
              + child.getTupleDesc().getFieldName(aggregateFieldIndex) + ")" });
    }
    return new TupleDesc(new Type[] { Type.INT_TYPE }, new String[] { aggregateOperator.toString()
        + " (" + child.getTupleDesc().getFieldName(aggregateFieldIndex) + ")" });
  }

  @Override
  public void close() {
    this.open = false;
    super.close();
    if (aggregateResultsIterator != null)
      aggregateResultsIterator.close();
  }

  @Override
  public DbIterator[] getChildren() {
    return new DbIterator[] { child };
  }

  @Override
  public void setChildren(DbIterator[] children) {
    if (this.open)
      throw new IllegalStateException("Cannot set child while open");
    this.child = children[0];
    resetAggregator();
  }

}
