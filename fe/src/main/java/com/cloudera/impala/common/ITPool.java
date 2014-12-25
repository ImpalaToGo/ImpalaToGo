package com.cloudera.impala.common;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** thin executor for misc tasks */
public class ITPool {
  private final ExecutorService _service;

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
   */
  public <T> T run(InterruptableCallable<T> callable, long timeout) throws InterruptedException, ExecutionException{
    Future<T> futureResult = _service.submit(callable);
    T result = null;
    try{
        result = futureResult.get(timeout, TimeUnit.MILLISECONDS);
    }catch(TimeoutException e){
        System.out.println("No response after one second");
        futureResult.cancel(true);
    }
    return result;
  }

}
