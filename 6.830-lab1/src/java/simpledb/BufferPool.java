package simpledb;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * BufferPool manages the reading and writing of pages into memory from disk.
 * Access methods call into it to retrieve pages, and it fetches pages from the
 * appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a
 * page, BufferPool checks that the transaction has the appropriate locks to
 * read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
  /** Bytes per page, including header. */
  private static final int PAGE_SIZE = 4096;

  /**
   * Default number of pages passed to the constructor. This is used by other
   * classes. BufferPool should use the numPages argument to the constructor
   * instead.
   */
  public static final int DEFAULT_PAGES = 50;

  private final int maxPages;
  private int currentPages;

  private final Map<PageId, Page> pageIdToPages;

  /**
   * Creates a BufferPool that caches up to numPages pages.
   * 
   * @param numPages maximum number of pages in this buffer pool.
   */
  public BufferPool(int numPages) {
    this.maxPages = numPages;
    this.pageIdToPages = new HashMap<PageId, Page>();
  }

  public static int getPageSize() {
    return PAGE_SIZE;
  }

  /**
   * Retrieve the specified page with the associated permissions. Will acquire a
   * lock and may block if that lock is held by another transaction.
   * <p>
   * The retrieved page should be looked up in the buffer pool. If it is
   * present, it should be returned. If it is not present, it should be added to
   * the buffer pool and returned. If there is insufficient space in the buffer
   * pool, an page should be evicted and the new page should be added in its
   * place.
   * 
   * @param tid the ID of the transaction requesting the page
   * @param pid the ID of the requested page
   * @param perm the requested permissions on the page
   */
  public Page getPage(TransactionId tid, PageId pid, Permissions perm) throws DbException {
    if (pageIdToPages.containsKey(pid)) {
      return pageIdToPages.get(pid);
    }
    if (currentPages == maxPages) {
      evictPage();
    }
    int tableId = pid.getTableId();
    Catalog catalog = Database.getCatalog();
    DbFile dbFile = catalog.getDatabaseFile(tableId);
    Page page = dbFile.readPage(pid);
    pageIdToPages.put(pid, page);
    currentPages++;
    return page;
  }

  /**
   * Releases the lock on a page. Calling this is very risky, and may result in
   * wrong behavior. Think hard about who needs to call this and why, and why
   * they can run the risk of calling it.
   * 
   * @param tid the ID of the transaction requesting the unlock
   * @param pid the ID of the page to unlock
   */
  public void releasePage(TransactionId tid, PageId pid) {
    // some code goes here
    // not necessary for lab1|lab2
  }

  /**
   * Release all locks associated with a given transaction.
   * 
   * @param tid the ID of the transaction requesting the unlock
   */
  public void transactionComplete(TransactionId tid) throws IOException {
    // some code goes here
    // not necessary for lab1|lab2
  }

  /** Return true if the specified transaction has a lock on the specified page */
  public boolean holdsLock(TransactionId tid, PageId p) {
    // some code goes here
    // not necessary for lab1|lab2
    return false;
  }

  /**
   * Commit or abort a given transaction; release all locks associated to the
   * transaction.
   * 
   * @param tid the ID of the transaction requesting the unlock
   * @param commit a flag indicating whether we should commit or abort
   */
  public void transactionComplete(TransactionId tid, boolean commit) throws IOException {
    // some code goes here
    // not necessary for lab1|lab2
  }

  /**
   * Add a tuple to the specified table behalf of transaction tid. Will acquire
   * a write lock on the page the tuple is added to(Lock acquisition is not
   * needed for lab2). May block if the lock cannot be acquired.
   * 
   * Marks any pages that were dirtied by the operation as dirty by calling
   * their markDirty bit, and updates cached versions of any pages that have
   * been dirtied so that future requests see up-to-date pages.
   * 
   * @param tid the transaction adding the tuple
   * @param tableId the table to add the tuple to
   * @param t the tuple to add
   */
  public void insertTuple(TransactionId tid, int tableId, Tuple t) throws DbException, IOException,
      TransactionAbortedException {
    HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
    List<Page> dirtiedPages = heapFile.insertTuple(tid, t);
    for (Page dirtiedPage : dirtiedPages) {
      dirtiedPage.markDirty(true, tid);
    }
  }

  /**
   * Remove the specified tuple from the buffer pool. Will acquire a write lock
   * on the page the tuple is removed from. May block if the lock cannot be
   * acquired.
   * 
   * Marks any pages that were dirtied by the operation as dirty by calling
   * their markDirty bit. Does not need to update cached versions of any pages
   * that have been dirtied, as it is not possible that a new page was created
   * during the deletion (note difference from addTuple).
   * 
   * @param tid the transaction deleting the tuple.
   * @param t the tuple to delete
   */
  public void deleteTuple(TransactionId tid, Tuple t) throws DbException,
      TransactionAbortedException {
    HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(
        t.getRecordId().getPageId().getTableId());
    Page dirtiedPage = heapFile.deleteTuple(tid, t);
    dirtiedPage.markDirty(true, tid);
  }

  /**
   * Flush all dirty pages to disk. NB: Be careful using this routine -- it
   * writes dirty data to disk so will break simpledb if running in NO STEAL
   * mode.
   */
  public synchronized void flushAllPages() throws IOException {
    for (PageId pageId : pageIdToPages.keySet()) {
      flushPage(pageId);
    }
  }

  /**
   * Remove the specific page id from the buffer pool. Needed by the recovery
   * manager to ensure that the buffer pool doesn't keep a rolled back page in
   * its cache.
   */
  public synchronized void discardPage(PageId pid) {
    // some code goes here
    // only necessary for lab5
  }

  /**
   * Flushes a certain page to disk
   * 
   * @param pid an ID indicating the page to flush
   */
  private synchronized void flushPage(PageId pid) throws IOException {
    if (pageIdToPages.containsKey(pid)) {
      Page page = pageIdToPages.get(pid);
      if (page.isDirty() != null) {
        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
        page.markDirty(false, null);
      }
    }
  }

  /**
   * Write all pages of the specified transaction to disk.
   */
  public synchronized void flushPages(TransactionId tid) throws IOException {
    for (PageId pageId : pageIdToPages.keySet()) {
      Page page = pageIdToPages.get(pageId);
      if (page.isDirty() == tid) {
        Database.getCatalog().getDatabaseFile(pageId.getTableId()).writePage(page);
        page.markDirty(false, null);
      }
    }
  }

  /**
   * Discards a page from the buffer pool. Flushes the page to disk to ensure
   * dirty pages are updated on disk.
   */
  private synchronized void evictPage() throws DbException {
    PageId pageId = pageIdToPages.keySet().iterator().next();
    if (pageId == null) return;
    try {
      flushPage(pageId);
    } catch (IOException e) {
      e.printStackTrace();
      throw new DbException("IOException while flushing page during eviction.");
    }
    pageIdToPages.remove(pageId);
    currentPages--;
  }

}
