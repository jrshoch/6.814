package simpledb;


/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

  private static final long serialVersionUID = 1L;

  private final TransactionId transactionId;
  private DbIterator child;
  private boolean open;
  private boolean deleted;
  /**
   * Constructor specifying the transaction that this delete belongs to as well
   * as the child to read from.
   * 
   * @param t The transaction this delete runs in
   * @param child The child operator from which to read tuples for deletion
   */
  public Delete(TransactionId t, DbIterator child) {
    this.transactionId = t;
    this.child = child;
    this.open = false;
    this.deleted = false;
  }

  @Override
  public TupleDesc getTupleDesc() {
    return new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"NUMBER DELETED"});
  }

  @Override
  public void open() throws DbException, TransactionAbortedException {
    super.open();
    this.open = true;
    this.deleted = false;
  }

  @Override
  public void close() {
    super.close();
    this.open = false;
  }

  @Override
  public void rewind() throws DbException, TransactionAbortedException {
    close();
    open();
  }

  /**
   * Deletes tuples as they are read from the child operator. Deletes are
   * processed via the buffer pool (which can be accessed via the
   * Database.getBufferPool() method.
   * 
   * @return A 1-field tuple containing the number of deleted records.
   * @see Database#getBufferPool
   * @see BufferPool#deleteTuple
   */
  @Override
  protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    if (deleted) {
      return null;
    }
    child.open();
    int count = 0;
    while (child.hasNext()) {
      Tuple toBeDeletedTuple = child.next();
      Database.getBufferPool().deleteTuple(transactionId, toBeDeletedTuple);
      count++;
    }
    child.close();
    Tuple resultTuple = new Tuple(getTupleDesc());
    resultTuple.setField(0, new IntField(count));
    this.deleted = true;
    return resultTuple;
  }

  @Override
  public DbIterator[] getChildren() {
    return new DbIterator[]{child};
  }

  @Override
  public void setChildren(DbIterator[] children) {
    if (open) {
      throw new IllegalStateException("Child cannot be set while open.");
    }
    this.child = children[0];
  }

}
