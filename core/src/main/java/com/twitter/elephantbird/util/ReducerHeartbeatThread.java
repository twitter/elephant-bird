package com.twitter.elephantbird.util;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Keeps a reducer alive during a slow close() method
 *
 * @author Alex Levenson
 */
public class ReducerHeartbeatThread {
  private final AtomicInteger latch = new AtomicInteger();
  private final Thread beat;

  /**
   * Creates a new thread to keep a reducer alive
   * Does not start the thread until you call {@link #start()}
   * @param context the task attempt context, used to send progress signals to the reducer
   * @param periodMillis how often to wake up and send a progress signal in milliseconds
   */
  public ReducerHeartbeatThread(final TaskAttemptContext context, final long periodMillis) {
    beat = new Thread(
      new Runnable() {
        @Override
        public void run() {
          boolean interrupted = false;
          while (latch.get() == 0 && !interrupted) {
            try {
              Thread.sleep(periodMillis);
            } catch (InterruptedException e) {
              interrupted = true;
            }

            // keep the reducer alive
            context.progress();

            // call the abstract progress method
            progress();
          }
        }
      });
  }

  /**
   * Same as {@link #ReducerHeartbeatThread(TaskAttemptContext, long)}
   * but with a default period of 1 minute
   */
  public ReducerHeartbeatThread(TaskAttemptContext context) {
    this(context, 60 * 1000);
  }

  /**
   * Keep the reducer alive until {@link #stop()} is called
   */
  public void start() {
    beat.start();
  }

  /**
   * Stop keeping the reducer alive, make sure to call this when your
   * close method is finished. It's a good idea to wrap your close()
   * with a {@code try { ... } finally {heartBeat.stop()}} to ensure that the
   * heartBeat is stopped. If the heartBeat remains alive failed jobs can remain 'running'
   * indefinitely.
   */
  public void stop() {
    latch.incrementAndGet();
    beat.interrupt();
  }

  /**
   * This will be called once every periodMillis.
   * Override to report progress of the slow
   * close() method
   */
  protected void progress() {}
}
