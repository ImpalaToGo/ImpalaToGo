package com.cloudera.impala.common;

import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

/** Represent interruptable callable. T is the result type */
public abstract class InterruptableCallable<T> implements Callable<T>{

  private static final Logger LOG = Logger.getLogger(InterruptableCallable.class);

  @Override
  public T call() throws Exception{
    try{
        return dowork();
    }catch(InterruptedException e){
      LOG.error("Thread was interrupted");
    }
    catch(Exception e){
      LOG.error("Generic exception happens : \"" + e.getMessage() + "\".");
    }
    return null;
  }

  /**
   * work executor
   * @return result
   *
   * @throws Exception
   */
  protected abstract T dowork() throws Exception;

}
