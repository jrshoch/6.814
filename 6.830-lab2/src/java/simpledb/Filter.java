package simpledb;

import java.util.NoSuchElementException;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

  private static final long serialVersionUID = 1L;

  private final Predicate p;
  private DbIterator child;
  private boolean open;
  
  /**
   * Constructor accepts a predicate to apply and a child operator to read
   * tuples to filter from.
   * 
   * @param p The predicate to filter tuples with
   * @param child The child operator
   */
  public Filter(Predicate p, DbIterator child) {
    this.p = p;
    this.child = child;
    this.open = false;
  }

  public Predicate getPredicate() {
    return p;
  }

  @Override
  public TupleDesc getTupleDesc() {
    return child.getTupleDesc();
  }

  @Override
  public void open() throws DbException, NoSuchElementException, TransactionAbortedException {
    super.open();
    child.open();
    this.open = true;
  }

  @Override
  public void close() {
    super.close();
    child.close();
    this.open = false;
  }

  @Override
  public void rewind() throws DbException, TransactionAbortedException {
    child.rewind();
  }

  /**
   * AbstractDbIterator.readNext implementation. Iterates over tuples from the
   * child operator, applying the predicate to them and returning those that
   * pass the predicate (i.e. for which the Predicate.filter() returns true.)
   * 
   * @return The next tuple that passes the filter, or null if there are no more
   *         tuples
   * @see Predicate#filter
   */
  @Override
  protected Tuple fetchNext() throws NoSuchElementException, TransactionAbortedException,
      DbException {
    while (child.hasNext()) {
      Tuple potentialTuple = child.next();
      if (p.filter(potentialTuple)) {
        return potentialTuple;
      }
    }
    return null;
  }

  @Override
  public DbIterator[] getChildren() {
    return new DbIterator[]{child};
  }

  @Override
  public void setChildren(DbIterator[] children) {
    if (this.open)
      throw new IllegalStateException("Cannot set children while open.");
    this.child = children[0];
  }

}
