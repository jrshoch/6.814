package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

  private static final long serialVersionUID = 1L;
  
  private final int tableId;
  private final TransactionId transactionId;
  private DbIterator child;
  private boolean open;
  private boolean inserted;
  /**
   * Constructor.
   * 
   * @param t The transaction running the insert.
   * @param child The child operator from which to read tuples to be inserted.
   * @param tableid The table in which to insert tuples.
   * @throws DbException if TupleDesc of child differs from table into which we
   *           are to insert.
   */
  public Insert(TransactionId t, DbIterator child, int tableid) throws DbException {
    this.transactionId = t;
    this.child = child;
    this.tableId = tableid;
    this.open = false;
  }

  @Override
  public TupleDesc getTupleDesc() {
    return new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"NUMBER INSERTED"});
  }

  @Override
  public void open() throws DbException, TransactionAbortedException {
    super.open();
    this.open = true;
    this.inserted = false;
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
   * Inserts tuples read from child into the tableid specified by the
   * constructor. It returns a one field tuple containing the number of inserted
   * records. Inserts should be passed through BufferPool. An instances of
   * BufferPool is available via Database.getBufferPool(). Note that insert DOES
   * NOT need check to see if a particular tuple is a duplicate before inserting
   * it.
   * 
   * @return A 1-field tuple containing the number of inserted records, or null
   *         if called more than once.
   * @see Database#getBufferPool
   * @see BufferPool#insertTuple
   */
  @Override
  protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    if (inserted) {
      return null;
    }
    child.open();
    int count = 0;
    while (child.hasNext()) {
      Tuple toBeInsertedTuple = child.next();
      try {
        Database.getBufferPool().insertTuple(transactionId, tableId, toBeInsertedTuple);
      } catch (IOException e) {
        e.printStackTrace();
        throw new DbException("IOException caught while trying to insert tuple.");
      }
      count++;
    }
    child.close();
    Tuple resultTuple = new Tuple(getTupleDesc());
    resultTuple.setField(0, new IntField(count));
    this.inserted = true;
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
