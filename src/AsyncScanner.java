/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package org.hbase.async;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.LoggerFactory;

import java.lang.Exception;
import java.lang.Object;
import java.lang.Override;
import java.lang.RuntimeException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncScanner {
  public static final org.slf4j.Logger log = LoggerFactory.getLogger(AsyncScanner.class);

  public static interface BatchProcessor {
    public Deferred<Object> process(ArrayList<ArrayList<KeyValue>> rows);
  }

  private final Scanner scanner;
  private final BatchProcessor batchProcessor;
  private final Callback<Object, Exception> errback;
  private final int max_in_flight;
  private final int batchSize;

  private final Deferred<Object> result = new Deferred<Object>();
  private final ScanLoop loop = new ScanLoop(new BatchCompletedCB());
  private final AtomicInteger inflight_slots;
  private volatile boolean done = false;

  public AsyncScanner(final Scanner scanner, final BatchProcessor batchProcessor, final int max_in_flight, final int batchSize) {
    this.scanner = scanner;
    this.batchProcessor = batchProcessor;
    this.max_in_flight = max_in_flight;
    this.inflight_slots = new AtomicInteger(max_in_flight);
    this.batchSize = batchSize;
    this.errback = new Callback<Object, Exception>() {
      @Override
      public Object call(Exception ex) throws Exception {
        result.callback(ex);
        return null;
      }
    };
  }

  // Callback that iterates asynchronously over the rows returned by the
  // scanner.
  class ScanLoop implements Callback<Object, ArrayList<ArrayList<KeyValue>>> {
    // What's the callback that should get triggered upon completion of a
    // batch of work.
    private final Callback<Object, Object> batchProcessedCb;

    ScanLoop(Callback<Object, Object> batchProcessedCb) {
      this.batchProcessedCb = batchProcessedCb;
    }

    void next() {
      //log.info("callback got : " + rows.toString());
      int slots_left = inflight_slots.get();
      assert(slots_left <= max_in_flight);
      assert(slots_left >= 0);
      if (slots_left != 0) {   // Can we go fetch more data?
        if(inflight_slots.compareAndSet(slots_left,slots_left-1)) {
          log.info("ScanLoop : " + slots_left + " inflight slot left : kicking off next batch");
          scanner.nextRows(batchSize).addCallback(ScanLoop.this).addErrback(errback);
        } else {
          log.info("ScanLoop : atomic integer race detected : pause");
        }
      } else {
        // else: already as many batches in flight as we allow,
        // so "pause" scanning by not kicking off another scan().
        log.info("ScanLoop : " + slots_left + " inflight slot left : pause");
      }
    }

    public Object call(final ArrayList<ArrayList<KeyValue>> rows) {
      if (rows == null) {  // We've reached the end of the scanner.
        done = true;       // So remember that here. We're not 100% done yet.
        int slots_left = inflight_slots.incrementAndGet(); // last scan also ate one slot
        log.info("ScanLoop : got null this is the end (slots_left=" + slots_left + ")");
        if(slots_left == max_in_flight){
          log.info("ScanLoop : all processing has been done triggering result callback");
          result.callback(null);
        }
        return null;       // some batches may still be in flight.
      }

      batchProcessor.process(rows).addCallback(batchProcessedCb).addErrback(errback);
      next();

      return null;
    }
  }

  // Whenever a batch of work completes, this callback gets executed.
  class BatchCompletedCB implements Callback<Object, Object> {
    public Object call(final Object unused) {
      // Start by signaling that a batch completed by incrementing our
      // counter (remember the counter indicates how many more batches
      // can still be scheduled according to our limit).
      int slots_left = inflight_slots.incrementAndGet();
      log.info("BatchCompletedCB : done=" + done + ", slots_left=" + slots_left);
      assert(slots_left <= max_in_flight);
      assert(slots_left >= 0);
      if (slots_left == 1 && !done) {
        // if we get here it's because slots_left was zero, and we're the
        // first ones to increment it so we got one.  If slots_left was
        // zero, it means that scanning had ceased as we had too many
        // batches in flight at the same time.  So we should resume scanning
        // at this point:
        log.info("BatchCompletedCB : not done so kicking off next batch");
        loop.next();
      } else if (slots_left == max_in_flight && done) {
        // if we get here it's because we're the last batch to complete: the
        // scanning loop indicated it was done scanning everything, and
        // we're the last one to increment the counter, thereby returning
        // its value to where it started from, at `max_in_flight'.
        log.info("BatchCompletedCB : this was the last batch triggering result callback");
        result.callback(null);  // So indicate our caller we're all done.
      }
      return null;
    }
  }

  public Deferred<Object> scanAndProcess() {
    log.info("scan start");
    loop.next(); // Kick off the whole dance.
    return result;
  }
}
