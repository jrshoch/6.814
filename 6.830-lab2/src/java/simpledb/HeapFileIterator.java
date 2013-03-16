package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {

  private final int tableId;
  private final int numberOfPages;
  private final TransactionId transactionId;
  private int currentPageNumber;
  private Iterator<Tuple> currentPageIterator;
  private boolean open;
  private Tuple next;

  private HeapFileIterator(int tableId, int numberOfPages, TransactionId transactionId) {
    this.tableId = tableId;
    this.numberOfPages = numberOfPages;
    this.open = false;
    this.transactionId = transactionId;
  }

  public static HeapFileIterator create(int tableId, int numberOfPages, TransactionId transactionId) {
    return new HeapFileIterator(tableId, numberOfPages, transactionId);
  }

  @Override
  public void open() throws DbException, TransactionAbortedException {
    if (open) {
      throw new DbException("Opened already open HeapFileIterator");
    }
    this.open = true;
    rewind();
  }

  private Iterator<Tuple> getPageIterator(int pageNumber) throws DbException {
    PageId pageId = new HeapPageId(tableId, pageNumber);
    Page page = Database.getBufferPool().getPage(transactionId, pageId, Permissions.READ_ONLY);
    return ((HeapPage) page).iterator();
  }

  private void incrementPageNumber() throws DbException {
    currentPageNumber++;
    if (currentPageNumber >= numberOfPages) {
      return;
    }
    currentPageIterator = getPageIterator(currentPageNumber);
  }

  private Tuple getNext() throws DbException {
    while (currentPageNumber < numberOfPages) {
      if (currentPageIterator.hasNext()) {
        return currentPageIterator.next();
      }
      incrementPageNumber();
    }
    return null;
  }

  @Override
  public boolean hasNext() throws DbException, TransactionAbortedException {
    return open && (next != null);
  }

  @Override
  public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
    if (!open || !hasNext()) {
      throw new NoSuchElementException();
    }
    Tuple result = next;
    next = getNext();
    return result;
  }

  @Override
  public void rewind() throws DbException, TransactionAbortedException {
    if (open) {
      this.currentPageNumber = 0;
      this.currentPageIterator = getPageIterator(currentPageNumber);
      this.next = getNext();
    }
  }

  @Override
  public void close() {
    open = false;
  }

}
