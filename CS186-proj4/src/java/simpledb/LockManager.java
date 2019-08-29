package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class LockManager{
    private ConcurrentHashMap<PageId, ArrayList<TransactionId>> share;
    private ConcurrentHashMap<PageId, TransactionId> exclude;

    public LockManager(){
        share = new ConcurrentHashMap<>();
        exclude = new ConcurrentHashMap<>();
    }

    public synchronized boolean hasSharedLock(PageId pid, TransactionId tid){
        return share.get(pid) != null && share.get(pid).contains(tid);
    }
    public synchronized boolean hasExclusiveLock(PageId pid, TransactionId tid){
        return exclude.get(pid) != null && exclude.get(pid).equals(tid);
    }

    public synchronized boolean getLock(Permissions perm, PageId pid, TransactionId tid){
        if(perm.equals(Permissions.READ_ONLY)){
            if(hasSharedLock(pid, tid) || hasExclusiveLock(pid, tid)) return true;
            if(exclude.get(pid) == null){
                if(share.get(pid) == null) share.put(pid, new ArrayList<TransactionId>());
                share.get(pid).add(tid);
                return true;
            }
            return false;
        }else{
        //    if(!tid.toString().equals(last))
          //    System.out.println("get an exclusiveLock for " + tid.toString());
            if(hasExclusiveLock(pid, tid)) return true;
            if(exclude.get(pid) != null){
                System.out.println("pid already exist!");
                return false;
            } 
            if(share.get(pid) == null || share.get(pid).size() == 0){
                exclude.put(pid, tid);
                return true;
            }
            if(share.get(pid).size() == 1 && share.get(pid).contains(tid)){
                share.get(pid).remove(tid);
                exclude.put(pid, tid);
                return true;
            }
            return false;
        }
    }

    public synchronized void releaseOneLock(PageId pid, TransactionId tid){
        if(exclude.get(pid) != null && exclude.get(pid).equals(tid)) exclude.remove(pid);
        if(share.get(pid) != null && share.get(pid).contains(tid)) share.get(pid).remove(tid);
    }

    public synchronized void releaseAllLocks(TransactionId tid){
        for(Map.Entry<PageId, TransactionId> entry : exclude.entrySet())
          if(entry.getValue().equals(tid)) exclude.remove(entry.getKey());
        for(Map.Entry<PageId, ArrayList<TransactionId>> entry : share.entrySet())
          if(entry.getValue().contains(tid)) entry.getValue().remove(tid);
    }

    public synchronized boolean holdsLock(PageId pid, TransactionId tid){
        if(share.get(pid) != null)
          return share.get(pid).contains(tid);
        if(exclude.get(pid) != null)
          return exclude.get(pid).equals(tid);
        return false;
    }

    public synchronized void debugPage(PageId pid){
        if(share.get(pid) != null) System.out.println("SharedLock still exist!");
        else System.out.println("SharedLock doesn't exist!");
        if(exclude.get(pid) != null) System.out.println("ExclusiveLock still exist!");
        else System.out.println("ExclusiveLock doesn't exist!");
    }
}