package com.cloudera.impala.common;

import java.util.concurrent.Callable;


public abstract class InterruptableCallable<T> implements Callable<T>{

  public T call() throws Exception{
    try{
        return dowork();
    }catch(InterruptedException e){
        System.out.println("Thread was interrupted");
    }
    catch(Exception e){
      System.out.println("Genereic exception happens : \"" + e.getMessage() + "\".");
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
