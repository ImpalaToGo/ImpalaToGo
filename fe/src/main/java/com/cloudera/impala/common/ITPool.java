package com.cloudera.impala.common;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

/** thin executor for misc tasks */
public class ITPool {
  private final ExecutorService _service;
  private static final Logger LOG = Logger.getLogger(ITPool.class);

  /**
   * Construct the pool
   * @param timeoutms - timeout in ms for managed tasks
   */
  public ITPool(){
    _service = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
  }

  /**
   * run specified task in a blocking way.
   *
   * @param  callable - callable unit
   * @return task result
   *
   * @throws InterruptedException  exception fires when task is interrupted due timeout elapsed
   * @throws ExecutionException    exception fires when underlying execution was aborted by throwing an exception
   * @throws TimeoutException      exception fires when timeout of waiting on the scheduled task is exceeded
   */
  public <T> T run(InterruptableCallable<T> callable, long timeout) throws InterruptedException, ExecutionException, TimeoutException{
    Future<T> futureResult = _service.submit(callable);
    T result = null;
    try{
        result = futureResult.get(timeout, TimeUnit.MILLISECONDS);
    }catch(TimeoutException e){
      LOG.error("Timeout \"" + timeout + "\" exceeded, cancelling task \"" + callable.getName() + "\"...");
      futureResult.cancel(true);
      throw e;
    }
    return result;
  }

}
