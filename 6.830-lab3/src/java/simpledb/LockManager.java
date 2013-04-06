package simpledb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

public class LockManager {

  private final ConcurrentMap<PageId, Object> locks;
  private final Map<PageId, List<TransactionId>> sharedLocks;
  private final Map<PageId, TransactionId> exclusiveLocks;
  private final ConcurrentMap<TransactionId, Collection<PageId>> pageIdsLockedByTransaction;

  private LockManager() {
    locks = new ConcurrentHashMap<PageId, Object>();
    sharedLocks = new HashMap<PageId, List<TransactionId>>();
    exclusiveLocks = new HashMap<PageId, TransactionId>();
    pageIdsLockedByTransaction = new ConcurrentHashMap<TransactionId, Collection<PageId>>();
  }

  public static LockManager create() {
    return new LockManager();
  }

  private Object getLock(PageId pageId) {
    locks.putIfAbsent(pageId, new Object());
    return locks.get(pageId);
  }

  public boolean acquireLock(TransactionId transactionId, PageId pageId, Permissions permissions) {
    if (permissions == Permissions.READ_ONLY) {
      if (hasReadPermissions(transactionId, pageId)) {
        return true;
      }
      while (!acquireReadOnlyLock(transactionId, pageId)) {
        // waiting for lock
      }
    } else if (permissions == Permissions.READ_WRITE) {
      if (hasWritePermissions(transactionId, pageId)) {
        return true;
      }
      while (!acquireReadWriteLock(transactionId, pageId)) {
        // waiting for lock
      }
    } else {
      throw new IllegalArgumentException("Expected either READ_ONLY or READ_WRITE permissions.");
    }
    addPageToTransactionLocks(transactionId, pageId);
    return true;
  }
  
  private boolean hasReadPermissions(TransactionId transactionId, PageId pageId) {
    if (hasWritePermissions(transactionId, pageId)) {
      return true;
    }
    return sharedLocks.containsKey(pageId) && sharedLocks.get(pageId).contains(transactionId);
  }
  
  private boolean hasWritePermissions(TransactionId transactionId, PageId pageId) {
    return exclusiveLocks.containsKey(pageId) && transactionId.equals(exclusiveLocks.get(pageId));
  }

  private void addPageToTransactionLocks(TransactionId transactionId, PageId pageId) {
    pageIdsLockedByTransaction.putIfAbsent(transactionId, new LinkedBlockingQueue<PageId>());
    pageIdsLockedByTransaction.get(transactionId).add(pageId);
  }

  public boolean acquireReadOnlyLock(TransactionId transactionId, PageId pageId) {
    Object lock = getLock(pageId);
    while (true) {
      synchronized (lock) {
        if (!isExclusivelyLockedByOthers(transactionId, pageId)) {
          addSharedUser(transactionId, pageId);
          return true;
        }
      }
    }
  }

  private boolean isExclusivelyLockedByOthers(TransactionId transactionId, PageId pageId) {
    return exclusiveLocks.containsKey(pageId)
        && (!exclusiveLocks.get(pageId).equals(transactionId));
  }

  private void addSharedUser(TransactionId transactionId, PageId pageId) {
    if (!sharedLocks.containsKey(pageId)) {
      sharedLocks.put(pageId, new ArrayList<TransactionId>());
    }
    sharedLocks.get(pageId).add(transactionId);
  }

  private boolean isLockedByOthers(TransactionId transactionId, PageId pageId) {
    if (isExclusivelyLockedByOthers(transactionId, pageId)) {
      return true;
    }
    if (sharedLocks.containsKey(pageId)) {
      for (TransactionId lockHoldingTransactionId : sharedLocks.get(pageId)) {
        if (!lockHoldingTransactionId.equals(transactionId)) {
          return true;
        }
      }
    }
    return false;
  }

  private void addExclusiveUser(TransactionId transactionId, PageId pageId) {
    exclusiveLocks.put(pageId, transactionId);
  }

  public boolean acquireReadWriteLock(TransactionId transactionId, PageId pageId) {
    Object lock = getLock(pageId);
    while (true) {
      synchronized (lock) {
        if (!isLockedByOthers(transactionId, pageId)) {
          addExclusiveUser(transactionId, pageId);
          return true;
        }
      }
    }
  }

  private void releaseLock(TransactionId transactionId, PageId pageId) {
    Object lock = getLock(pageId);
    synchronized (lock) {
      exclusiveLocks.remove(pageId);
      if (sharedLocks.containsKey(pageId)) {
        sharedLocks.get(pageId).remove(transactionId);
      }
    }
  }

  public void releasePage(TransactionId transactionId, PageId pageId) {
    releaseLock(transactionId, pageId);
    if (pageIdsLockedByTransaction.containsKey(transactionId)) {
      pageIdsLockedByTransaction.get(transactionId).remove(pageId);
    }
  }

  public void releasePages(TransactionId transactionId) {
    if (pageIdsLockedByTransaction.containsKey(transactionId)) {
      Collection<PageId> pageIds = pageIdsLockedByTransaction.get(transactionId);
      for (PageId pageId : pageIds) {
        releaseLock(transactionId, pageId);
      }
      pageIdsLockedByTransaction.replace(transactionId, new LinkedBlockingQueue<PageId>());
    }
  }

  public boolean holdsLock(TransactionId transactionId, PageId pageId) {
    if (!pageIdsLockedByTransaction.containsKey(transactionId)) {
      return false;
    }
    return pageIdsLockedByTransaction.get(transactionId).contains(pageId);
  }
}
