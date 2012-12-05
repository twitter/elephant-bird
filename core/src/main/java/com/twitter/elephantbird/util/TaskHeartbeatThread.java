package com.twitter.elephantbird.util;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Keeps a task alive during any slow operations
 *
 * @author Alex Levenson
 */
public class TaskHeartbeatThread {
  private final AtomicInteger latch = new AtomicInteger();
  private final Thread beat;

  /**
   * Creates a new thread to keep a task alive
   * Does not start the thread until you call {@link #start()}
   * @param context the task attempt context, used to send progress signals to the task
   * @param periodMillis how often to wake up and send a progress signal in milliseconds
   */
  public TaskHeartbeatThread(final TaskAttemptContext context, final long periodMillis) {
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

            // keep the task alive
            context.progress();

            // call the optional custom progress method
            progress();
          }
        }
      });
  }

  /**
   * Same as {@link #TaskHeartbeatThread(TaskAttemptContext, long)}
   * but with a default period of 1 minute
   */
  public TaskHeartbeatThread(TaskAttemptContext context) {
    this(context, 60 * 1000);
  }

  /**
   * Keep the task alive until {@link #stop()} is called
   */
  public void start() {
    beat.start();
  }

  /**
   * Stop keeping the task alive, make sure to call this when your
   * slow operation is finished. It's a good idea to wrap your slow code
   * with a <code>try { ... } finally {heartBeat.stop()}}</code> to ensure that the
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
