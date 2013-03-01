package simpledb;

import java.util.NoSuchElementException;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

  private final TupleDesc tupleDesc;
  private final DbFileIterator iterator;

  /**
   * Creates a sequential scan over the specified table as a part of the
   * specified transaction.
   * 
   * @param tid The transaction this scan is running as a part of.
   * @param tableid the table to scan.
   * @param tableAlias the alias of this table (needed by the parser); the
   *          returned tupleDesc should have fields with name
   *          tableAlias.fieldName (note: this class is not responsible for
   *          handling a case where tableAlias or fieldName are null. It
   *          shouldn't crash if they are, but the resulting name can be
   *          null.fieldName, tableAlias.null, or null.null).
   */
  public SeqScan(TransactionId tid, int tableId, String tableAlias) {
    this.tupleDesc = getPrefixedTupleDesc(tableId, tableAlias);
    this.iterator = Database.getCatalog().getDbFile(tableId).iterator(tid);
  }

  private static TupleDesc getPrefixedTupleDesc(int tableId, String tableAlias) {
    String tableAliasRepresentation = representPossiblyNullString(tableAlias);
    TupleDesc underlyingTupleDesc = Database.getCatalog().getTupleDesc(tableId);
    int tupleDescSize = underlyingTupleDesc.numFields();
    Type[] newTypes = new Type[tupleDescSize];
    String[] newFieldNames = new String[tupleDescSize];
    for (int i = 0; i < tupleDescSize; i++) {
      Type type = underlyingTupleDesc.getType(i);
      String fieldName = underlyingTupleDesc.getFieldName(i);
      newTypes[i] = type;
      newFieldNames[i] = tableAliasRepresentation + "." + representPossiblyNullString(fieldName);
    }
    return new TupleDesc(newTypes, newFieldNames);
  }

  private static String representPossiblyNullString(String string) {
    return (string == null) ? "null" : string;
  }

  @Override
  public void open() throws DbException, TransactionAbortedException {
    this.iterator.open();
  }

  /**
   * Returns the TupleDesc with field names from the underlying HeapFile,
   * prefixed with the tableAlias string from the constructor.
   * 
   * @return the TupleDesc with field names from the underlying HeapFile,
   *         prefixed with the tableAlias string from the constructor.
   */
  @Override
  public TupleDesc getTupleDesc() {
    return tupleDesc;
  }

  @Override
  public boolean hasNext() throws TransactionAbortedException, DbException {
    return this.iterator.hasNext();
  }

  @Override
  public Tuple next() throws NoSuchElementException, TransactionAbortedException, DbException {
    return this.iterator.next();
  }

  @Override
  public void close() {
    this.iterator.close();
  }

  @Override
  public void rewind() throws DbException, NoSuchElementException, TransactionAbortedException {
    this.iterator.rewind();
  }
}
