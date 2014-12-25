package com.cloudera.impala.catalog;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.cloudera.impala.common.ITPool;
import com.cloudera.impala.common.InterruptableCallable;

public class HadoopFsBridge {
  public static enum BridgeOpStatus{
    OK,
    TIMEOUT,
    FAILURE,
    NOT_RUN
  }

  private static ITPool pool = new ITPool();
  private static final Logger LOG = Logger.getLogger(HadoopFsBridge.class);

  public class BridgeOpResult<T>{
        T result;
        BridgeOpStatus status;

        public T getResult() { return result; }
        void setResult(T res) { result = res; }

        public BridgeOpStatus getStatus() { return status; }
        void setStatus(BridgeOpStatus stat) { status = stat; }
  }

  private <T> BridgeOpStatus run(InterruptableCallable<T> callable, AtomicReference<BridgeOpResult<T>> result, long timeout,
      String messageInterruptedEx, String messageExexEx, int retries){
    BridgeOpStatus status = null;
    T res = null;

    BridgeOpResult<T> temp = new BridgeOpResult<T>();
    temp.setResult(null);
    temp.setStatus(BridgeOpStatus.NOT_RUN);

    boolean go = true;
    while(go){
      try {
        res = pool.run(callable, timeout);
        temp.setStatus(BridgeOpStatus.OK);
        temp.setResult(res);
        }
      catch (InterruptedException e) {
        temp.setStatus(BridgeOpStatus.TIMEOUT);
        LOG.error(messageExexEx + "Ex : \"" + e.getMessage() + "\"." );
        e.printStackTrace();
        }
      catch (ExecutionException e) {
        temp.setStatus(BridgeOpStatus.FAILURE);
        LOG.error(messageInterruptedEx + "Ex : \"" + e.getMessage() + "\"." );
        }

      status = temp.getStatus();
      switch(status){
      case FAILURE:
      case OK:
        go = false;
        break;

      case TIMEOUT:
        if(--retries == 0)
          go = false;
        else{
          // sleep a bit and retry
          try {
            Thread.sleep(2000);
          } catch (InterruptedException e) {
            go = false;
          }
        }
        break;
      default:
        break;
      }
    }
    // reassign output result finally when retries exceeded
    result.set(temp);
    return status;
  }

  public BridgeOpResult<Boolean> exists(final FileSystem fs, final Path path, int retries, int timeout){
    AtomicReference<BridgeOpResult<Boolean>> result = new AtomicReference<BridgeOpResult<Boolean>>();

    //declaration of the anonymous class
    InterruptableCallable<Boolean> callable = new InterruptableCallable<Boolean>() {
      @Override
      protected Boolean dowork() throws IOException{
        return fs.exists(path);
      }
    };

    String messageInterruptedEx = "Timeout exception in \"exists\" operation for \"" + path.toString() +
        "\" on filesystem \"" + fs.getUri() + "\". ";
    String messageExexEx = "Execution exception in \"exists\" operation for \"" + path.toString() +
        "\" on filesystem \"" + fs.getUri() + "\". ";

    // run specified task with retries (we only make retries on timed out tasks):
    BridgeOpStatus status = run(callable, result, timeout, messageInterruptedEx, messageExexEx, retries);
    LOG.info("\"exists\" finished with status \"" + status + "\" for Path \"" + path + "\" on fs \"" + fs.getUri() + "\".");
    return result.get();
  }

  public BridgeOpResult<FileSystem> getFilesystem(final Path path, final Configuration configuration, int retries, int timeout){
    AtomicReference<BridgeOpResult<FileSystem>> result = new AtomicReference<BridgeOpResult<FileSystem>>();

    //declaration of the anonymous class
    InterruptableCallable<FileSystem> callable = new InterruptableCallable<FileSystem>() {
      @Override
      protected FileSystem dowork() throws IOException{
        return path.getFileSystem(configuration);
      }
    };

    String messageInterruptedEx = "Timeout exception in \"Path.getFilesystem\" operation for \"" + path.toString() + "\". ";
    String messageExexEx = "Execution exception in \"Path.getFilesystem\" operation for \"" + path.toString() + "\". ";

    // run specified task with retries (we only make retries on timed out tasks):
    BridgeOpStatus status = run(callable, result, timeout, messageInterruptedEx, messageExexEx, retries);
    LOG.info("\"getFilesystem\" finished with status \"" + status + "\" for Path \"" + path + "\".");
    return result.get();
  }

}
