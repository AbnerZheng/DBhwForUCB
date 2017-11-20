package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.io.Page;

import java.lang.reflect.Array;
import java.util.*;


public class SortOperator  {
  private Database.Transaction transaction;
  private String tableName;
  private Comparator<Record> comparator;
  private Schema operatorSchema;
  private int numBuffers;

  public SortOperator(Database.Transaction transaction, String tableName, Comparator<Record> comparator) throws DatabaseException, QueryPlanException {
    this.transaction = transaction;
    this.tableName = tableName;
    this.comparator = comparator;
    this.operatorSchema = this.computeSchema();
    this.numBuffers = this.transaction.getNumMemoryPages();
  }

  public Schema computeSchema() throws QueryPlanException {
    try {
      return this.transaction.getFullyQualifiedSchema(this.tableName);
    } catch (DatabaseException de) {
      throw new QueryPlanException(de);
    }
  }


  public class Run {
    private String tempTableName;

    public Run() throws DatabaseException {
      this.tempTableName = SortOperator.this.transaction.createTempTable(SortOperator.this.operatorSchema);
    }

    public void addRecord(List<DataBox> values) throws DatabaseException {
      SortOperator.this.transaction.addRecord(this.tempTableName, values);
    }

    public void addRecords(List<Record> records) throws DatabaseException {
      for (Record r: records) {
        this.addRecord(r.getValues());
      }
    }

    public Iterator<Record> iterator() throws DatabaseException {
      return SortOperator.this.transaction.getRecordIterator(this.tempTableName);
    }

    public String tableName() {
      return this.tempTableName;
    }
  }


  /**
   * Returns a NEW run that is the sorted version of the input run.
   * Can do an in memory sort over all the records in this run
   * using one of Java's built-in sorting methods.
   * Note: Don't worry about modifying the original run.
   * Returning a new run would bring one extra page in memory beyond the
   * size of the buffer, but it is done this way for ease.
   */
  public Run sortRun(Run run) throws DatabaseException {
    final Iterator<Record> iterator = run.iterator();
    List<Record> toSort = new ArrayList<>();
    while (iterator.hasNext()){
      toSort.add(iterator.next());
    }
    toSort.sort(comparator);
    final Run run1 = new Run();
    run1.addRecords(toSort);
    return run1;
  }



  /**
   * Given a list of sorted runs, returns a new run that is the result
   * of merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
   * to determine which record should be should be added to the output run next.
   * It is recommended that your Priority Queue hold Pair<Record, Integer> objects
   * where a Pair (r, i) is the Record r with the smallest value you are
   * sorting on currently unmerged from run i.
   */
  public Run mergeSortedRuns(List<Run> runs) throws DatabaseException {
    final PriorityQueue<Pair<Record, Integer>> pairPriorityQueue = new PriorityQueue<>(new Comparator<Pair<Record, Integer>>() {
      @Override
      public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
        return comparator.compare(o1.getFirst(), o2.getFirst());
      }
    });
    List<Iterator<Record>> runIterators = new ArrayList<>();
    int j = 0;
    for (int i=0; i< runs.size(); i++) {
      final Run run = runs.get(i);
      final Iterator<Record> iterator = run.iterator();
      if(iterator.hasNext()) {
        pairPriorityQueue.add(new Pair<>(iterator.next(), j));
        j++;
        runIterators.add(iterator);
      }
    }

    final Run retRun = new Run();
    while (!pairPriorityQueue.isEmpty()){
      final Pair<Record, Integer> poll = pairPriorityQueue.poll();
      retRun.addRecord(poll.getFirst().getValues());
      final Iterator<Record> recordIterator = runIterators.get(poll.getSecond());
      if(recordIterator.hasNext()){
        pairPriorityQueue.add(new Pair<>(recordIterator.next(), poll.getSecond()));
      }
    }
    return retRun;

  }

  /**
   * Given a list of N sorted runs, returns a list of
   * sorted runs that is the result of merging (numBuffers - 1)
   * of the input runs at a time.
   */
  public List<Run> mergePass(List<Run> runs) throws DatabaseException {
    final int numMergePass = SortOperator.this.transaction.getNumMemoryPages() - 1;
    final int length = (int) Math.ceil(1.0 * runs.size() / numMergePass);
    List<Run> retRuns = new ArrayList<>();
    for (int i = 0; i < numMergePass; i++) {
      List<Run> temp = new ArrayList<>();
      for (int j = 0; j < length && (i*length +j < runs.size()); j++) {
        temp.add(runs.get(i* length + j));
      }
      final Run run = mergeSortedRuns(temp);
      retRuns.add(run);
    }
    return retRuns;
  }


  /**
   * Does an external merge sort on the table with name tableName
   * using numBuffers.
   * Returns the name of the table that backs the final run.
   */
  public String sort() throws DatabaseException {
    BacktrackingIterator<Page> pageIterator = SortOperator.this.transaction.getPageIterator(SortOperator.this.tableName);
    pageIterator.next(); // skip header page
    List<Run> sortedRun = new ArrayList<>();
    while (pageIterator.hasNext()){
      final BacktrackingIterator<Record> blockIterator = transaction.getBlockIterator(tableName, pageIterator, 1);
      final Run run = new Run();
      while (blockIterator.hasNext()){
        final List<DataBox> values = blockIterator.next().getValues();
        run.addRecord(values);
      }
      final Run sortRun = sortRun(run);
      sortedRun.add(sortRun);
    }
    sortedRun = mergePass(sortedRun);
    final Run run = mergeSortedRuns(sortedRun);
    return run.tableName();
  }


  private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
    public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
      return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());

    }
  }

  public Run createRun() throws DatabaseException {
    return new Run();
  }



}
