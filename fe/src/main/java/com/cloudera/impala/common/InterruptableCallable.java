package com.cloudera.impala.common;

import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

/** Represent interruptable callable. T is the result type */
public abstract class InterruptableCallable<T> implements Callable<T>{

  private static final Logger LOG = Logger.getLogger(InterruptableCallable.class);

  /** Job name */
  private final String _name;

  public InterruptableCallable(String name) {
    _name = name;
  }

  /** getter for callable name, for logging identification */
  public String getName(){
    return _name;
  }

  @Override
  public T call() throws Exception{
    long startTime = System.nanoTime();
    try{
        return dowork();
    }catch(InterruptedException e){
      LOG.error("Thread was interrupted");
    }
    catch(Exception e){
      LOG.error("Generic exception happens : \"" + e.getMessage() + "\".");
    }
    finally{
      long estimatedTime = System.nanoTime() - startTime;
      LOG.info("Interruptable task \"" + _name + "\" took \"" + estimatedTime + "\" nanoseconds.\n");
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
