import sun.tools.jconsole.Tab;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.locks.Lock;

/**
 * The Lock Manager handles lock and unlock requests from transactions. The
 * Lock Manager will maintain a hashtable that is keyed on the name of the
 * table being locked. The Lock Manager will also keep a FIFO queue of requests
 * for locks that cannot be immediately granted.
 */
public class LockManager {
    private DeadlockAvoidanceType deadlockAvoidanceType;
    private HashMap<String, TableLock> tableToTableLock;

    public enum DeadlockAvoidanceType {
        None,
        WaitDie,
        WoundWait
    }

    public enum LockType {
        Shared,
        Exclusive
    }

    public LockManager(DeadlockAvoidanceType type) {
        this.deadlockAvoidanceType = type;
        this.tableToTableLock = new HashMap<String, TableLock>();
    }

    /**
     * The acquire method will grant the lock if it is compatible. If the lock
     * is not compatible, then the request will be placed on the requesters
     * queue. Once you have implemented deadlock avoidance algorithms, you
     * should instead check the deadlock avoidance type and call the
     * appropriate function that you will complete in part 2.
     * @param transaction that is requesting the lock
     * @param tableName of requested table
     * @param lockType of requested lock
     */
    public void acquire(Transaction transaction, String tableName, LockType lockType)
            throws IllegalArgumentException {
    	if(transaction.getStatus() == Transaction.Status.Waiting){
    	    throw new IllegalArgumentException();
        }
    	if(!this.tableToTableLock.containsKey(tableName)){
            tableToTableLock.put(tableName, new TableLock(lockType, transaction));
        }else {
            final TableLock tableLock = this.tableToTableLock.get(tableName);
            if(!compatible(tableName, transaction, lockType)){
                final Request e = new Request(transaction, lockType);
                transaction.sleep();
                if(tableLock.lockOwners.contains(tableName)){// 如果是升级锁
                    tableLock.requestersQueue.addFirst(e);
                }else {
                    tableLock.requestersQueue.addLast(e);
                }
            }else{
                tableLock.lockType = lockType;
                tableLock.lockOwners.add(transaction);
            }
        }
    }

    /**
     * This method will return true if the requested lock is compatible. See
     * spec provides compatibility conditions.
     * @param tableName of requested table
     * @param transaction requesting the lock
     * @param lockType of the requested lock
     * @return true if the lock being requested does not cause a conflict
     */
    private boolean compatible(String tableName, Transaction transaction, LockType lockType) throws IllegalArgumentException{
        TableLock tableLock = tableToTableLock.get(tableName);
        if(lockType.equals(LockType.Shared)){
           if(tableLock.lockType.equals(LockType.Shared)){
               return true;
           }else{
               if(tableLock.lockOwners.contains(transaction)){
                   throw new IllegalArgumentException();
               }
               return false;
           }
        }else{
            if(tableLock.lockType.equals(LockType.Shared)){
                if(tableLock.lockOwners.contains(transaction) && tableLock.lockOwners.size() == 1){
                    return true;
                }
                return false;
            }else if(tableLock.lockOwners.contains(transaction)){
        		throw new IllegalArgumentException();
            }
            return false;
        }
    }

    /**
     * Will release the lock and grant all mutually compatible transactions at
     * the head of the FIFO queue. See spec for more details.
     * @param transaction releasing lock
     * @param tableName of table being released
     */
    public void release(Transaction transaction, String tableName) throws IllegalArgumentException{
        if(transaction.getStatus() == Transaction.Status.Waiting){
            throw new IllegalArgumentException();
        }
        final TableLock tableLock = tableToTableLock.get(tableName);
        if(tableLock == null){
            throw new IllegalArgumentException();
        }
        final boolean remove = tableLock.lockOwners.remove(transaction);
        if(!remove){
            throw new IllegalArgumentException();
        }
        if(tableLock.lockOwners.size()==1 && tableLock.lockType.equals(LockType.Shared) && !tableLock.requestersQueue.isEmpty()){
        	for(int i = 0; i < tableLock.requestersQueue.size(); i++){
                Request request = tableLock.requestersQueue.get(i);
                if(tableLock.lockOwners.contains(request.transaction) && request.lockType.equals(LockType.Exclusive)){
                	tableLock.lockType = LockType.Exclusive;
                	tableLock.requestersQueue.remove(i);
                    request.transaction.wake();
        	        break;
                }
            }
        }else if(tableLock.lockType.equals(LockType.Shared) && !tableLock.lockOwners.isEmpty()){
        	int i = 0;
        	while(i < tableLock.requestersQueue.size()){
                final Request request = tableLock.requestersQueue.get(i);
                if(request.lockType == LockType.Shared){
                    tableLock.requestersQueue.remove(i);
                    tableLock.lockOwners.add(request.transaction);
                    request.transaction.wake();
                }else{
                    i++;
                }
            }
        }else if(tableLock.lockOwners.isEmpty() && tableLock.requestersQueue.isEmpty()){
            tableToTableLock.remove(tableName);
        }else if(tableLock.lockOwners.isEmpty()){
            final Request request = tableLock.requestersQueue.pollFirst();
            request.transaction.wake();
            tableLock.lockType = request.lockType;
            tableLock.lockOwners.add(request.transaction);
            switch (request.lockType){
                case Exclusive:
                    break;
                case Shared:
                	while(!tableLock.requestersQueue.isEmpty() &&  tableLock.requestersQueue.getFirst().lockType == LockType.Shared){
                        final Request request1 = tableLock.requestersQueue.pollFirst();
                        tableLock.lockOwners.add(request1.transaction);
                        request1.transaction.wake();
                    }
                	break;
            }
        }
    }

    /**
     * Will return true if the specified transaction holds a lock of type
     * lockType on the table tableName.
     * @param transaction holding lock
     * @param tableName of locked table
     * @param lockType of lock
     * @return true if the transaction holds lock
     */
    public boolean holds(Transaction transaction, String tableName, LockType lockType) {
        if(this.tableToTableLock.containsKey(tableName)){
            final TableLock tableLock = this.tableToTableLock.get(tableName);
            return tableLock.lockType.equals(lockType) && tableLock.lockOwners.contains(transaction);
        }
        return false;
    }

    /**
     * If transaction t1 requests an incompatible lock, t1 will abort if it has
     * a lower priority (higher timestamp) than all conflicting transactions.
     * If t1 has a higher priority, it will wait on the requesters queue.
     * @param tableName of locked table
     * @param transaction requesting lock
     * @param lockType of request
     */
    private void waitDie(String tableName, Transaction transaction, LockType lockType) {
        //TODO: HW5 Implement
    }

    /**
     * If transaction t1 requests an incompatible lock, t1 will wait if it has
     * a lower priority (higher timestamp) than conflicting transactions. If t1
     * has a higher priority than every conflicting transaction, it will abort
     * all the lock holders and acquire the lock.
     * @param tableName of locked table
     * @param transaction requesting lock
     * @param lockType of request
     */
    private void woundWait(String tableName, Transaction transaction, LockType lockType) {
        //TODO: HW5 Implement
    }

    /**
     * Contains all information about the lock for a specific table. This
     * information includes lock type, lock owner(s), and lock requestor(s).
     */
    private class TableLock {
        private LockType lockType;
        private HashSet<Transaction> lockOwners;
        private LinkedList<Request> requestersQueue;

        public TableLock(LockType lockType) {
            this.lockType = lockType;
            this.lockOwners = new HashSet<>();
            this.requestersQueue = new LinkedList<>();
        }

        public TableLock(LockType lockType, Transaction transaction){
            this(lockType);
            addLockOwner(transaction);
        }

        public void addLockOwner(Transaction lockOwner) {
            this.lockOwners.add(lockOwner);
        }
    }

    /**
     * Used to create request objects containing the transaction and lock type.
     * These objects will be added to the requestor queue for a specific table
     * lock.
     */
    private class Request {
        private Transaction transaction;
        private LockType lockType;

        public Request(Transaction transaction, LockType lockType) {
            this.transaction = transaction;
            this.lockType = lockType;
        }
    }
}
