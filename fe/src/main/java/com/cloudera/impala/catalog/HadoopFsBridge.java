package com.cloudera.impala.catalog;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStorageLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.Logger;

import com.cloudera.impala.common.ITPool;
import com.cloudera.impala.common.InterruptableCallable;

/** Hadoop FileSystem bridge.
 *  Controls FileSystem APIs invocation : handling hung invocation
 */
public class HadoopFsBridge {

  private HadoopFsBridge(){  }

  /** bridged operation status */
  public static enum BridgeOpStatus{
    OK,
    TIMEOUT,
    FAILURE,
    NOT_RUN
  }

  /** exponential base delay, for simplicity we use ( base * 2 * retry_number ) on retry */
  private static final long EXP_DELAY_BASE = 2000;

  /** remote API call permitted timeout */
  private static final long TIMEOUT_BASE = 20000;

  /** number of retries should be attempted while delegated API invocation is timed out */
  private static final int RETRIES = 5;

  /** Managed FileSystem API pool executor */
  private static ITPool pool = new ITPool();

  /** Logging mechanism */
  private static final Logger LOG = Logger.getLogger(HadoopFsBridge.class);

  /** Bridged operation result */
  public class BridgeOpResult<T>{

    /** error message */
    String error;

    /** operation result */
    T result;

    /** operation status */
    BridgeOpStatus status;

    /** error getter */
    public String getError() { return error; }

    /** error setter */
    public void setError(String err) { error = err; }

    /** get operation result */
    public T getResult() { return result; }

    /** set operation result*/
    void setResult(T res) { result = res; }

    /** get operation status */
    public BridgeOpStatus getStatus() { return status; }

    /** set operation status */
    void setStatus(BridgeOpStatus stat) { status = stat; }
  }

  /**
   * run the constructed callable in a managed way, controlling its lifecycle
   *
   * @param callable             - callable to invoke
   * @param result               - operation compound result
   * @param timeout              - operation timeout
   * @param messageInterruptedEx - message to print when "InterruptedException" happens
   * @param messageExexEx        - message to print when "ExecutionException" happens
   * @param retries              - number of retries operation will be reinvoked in case was timed out
   *
   * @return operation result
   */
  private static <T> BridgeOpStatus run(InterruptableCallable<T> callable, AtomicReference<BridgeOpResult<T>> result, long timeout,
      String messageInterruptedEx, String messageExexEx, int retries){
    BridgeOpStatus status = null;
    T res = null;

    int countdown = 0;

    BridgeOpResult<T> temp = new HadoopFsBridge().new BridgeOpResult<T>();
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
        LOG.error(messageInterruptedEx + "Ex : \"" + e.getMessage() + "\"; cause = \"" + e.getCause().getMessage() + "\"." );
        }

      status = temp.getStatus();
      switch(status){
      case OK:
        go = false;
        break;

      case TIMEOUT:
      case FAILURE:
        if(--retries == 0)
          go = false;
        else{
          // sleep a bit and retry
          try {
            Thread.sleep(EXP_DELAY_BASE * (2 * countdown++));
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

  /**
   * Execute FileSystem.exists(Path path) in the controlled way
   *
   * @param fs      - hadoop FileSystem
   * @param path    - hadoop Path
   *
   * @return operation compound result, if OK, contain Boolean
   */
  public static BridgeOpResult<Boolean> exists(final FileSystem fs, final Path path){
    AtomicReference<BridgeOpResult<Boolean>> result = new AtomicReference<BridgeOpResult<Boolean>>();

    //declaration of the anonymous class
    InterruptableCallable<Boolean> callable = new InterruptableCallable<Boolean>() {
      @Override
      protected Boolean dowork() throws IOException{
        return fs.exists(path);
      }
    };

    String messageInterruptedEx = "Timeout exception in \"FileSystem.exists\" operation for \"" + path +
        "\" on filesystem \"" + fs.getUri() + "\". ";
    String messageExexEx = "Execution exception in \"FileSystem.exists\" operation for \"" + path +
        "\" on filesystem \"" + fs.getUri() + "\". ";

    // run specified task with retries (we only make retries on timed out tasks):
    BridgeOpStatus status = run(callable, result, TIMEOUT_BASE, messageInterruptedEx, messageExexEx, RETRIES);
    LOG.info("\"FileSystem.exists\" finished with status \"" + status + "\" for Path \"" + path + "\" on fs \"" + fs.getUri() + "\".");
    return result.get();
  }

  /**
   * Execute Path.getFileSystem(Configuration configuration) in the controlled way
   *
   * @param path          - hadoop Path
   * @param configuration - hadoop Configuration
   *
   * @return operation compound result, if OK, contain the FileSystem
   */
  public static BridgeOpResult<FileSystem> getFilesystem(final Path path, final Configuration configuration){
    AtomicReference<BridgeOpResult<FileSystem>> result = new AtomicReference<BridgeOpResult<FileSystem>>();

    //declaration of the anonymous class
    InterruptableCallable<FileSystem> callable = new InterruptableCallable<FileSystem>() {
      @Override
      protected FileSystem dowork() throws IOException{
        return path.getFileSystem(configuration);
      }
    };

    String messageInterruptedEx = "Timeout exception in \"Path.getFilesystem\" operation for \"" + path + "\". ";
    String messageExexEx = "Execution exception in \"Path.getFilesystem\" operation for \"" + path + "\". ";

    // run specified task with retries (we only make retries on timed out tasks):
    BridgeOpStatus status = run(callable, result, RETRIES, messageInterruptedEx, messageExexEx, RETRIES);
    LOG.info("\"Path.getFilesystem\" finished with status \"" + status + "\" for Path \"" + path + "\".");
    return result.get();
  }

  /**
   * Execute FileSystem.listStatus(Path path) in the controlled way
   *
   * @param fs   - hadoop FileSystem
   * @param path - hadoop Path
   *
   * @return operation compound result, if status is OK, contain list of FileStatus found on the path
   */
  public static BridgeOpResult<FileStatus[]> listStatus(final FileSystem fs, final Path path){
    AtomicReference<BridgeOpResult<FileStatus[]>> result = new AtomicReference<BridgeOpResult<FileStatus[]>>();

    //declaration of the anonymous class
    InterruptableCallable<FileStatus[]> callable = new InterruptableCallable<FileStatus[]>() {
      @Override
      protected FileStatus[] dowork() throws IOException{
        return fs.listStatus(path);
      }
    };

    String messageInterruptedEx = "Timeout exception in \"FileSystem.listStatus\" operation for \"" + path +
        "\" on filesystem \"" + fs.getUri() + "\". ";
    String messageExexEx = "Execution exception in \"FileSystem.listStatus\" operation for \"" + path +
        "\" on filesystem \"" + fs.getUri() + "\". ";

    // run specified task with retries (we only make retries on timed out tasks):
    BridgeOpStatus status = run(callable, result, RETRIES, messageInterruptedEx, messageExexEx, RETRIES);
    LOG.info("\"FileSystem.listStatus\" finished with status \"" + status + "\" for Path \"" + path +
        "\" on fs \"" + fs.getUri() + "\".");
    return result.get();
  }

  /**
   * Execute FileSystem.getFileStatus(Path path) in the controlled way
   *
   * @param fs   - hadoop FileSystem
   * @param path - hadoop Path
   *
   * @return operation compound result, if status is OK, contain FileStatus
   */
  public static BridgeOpResult<FileStatus> getFileStatus(final FileSystem fs, final Path path){
    AtomicReference<BridgeOpResult<FileStatus>> result = new AtomicReference<BridgeOpResult<FileStatus>>();

    //declaration of the anonymous class
    InterruptableCallable<FileStatus> callable = new InterruptableCallable<FileStatus>() {
      @Override
      protected FileStatus dowork() throws IOException{
        return fs.getFileStatus(path);
      }
    };

    String messageInterruptedEx = "Timeout exception in \"FileSystem.getFileStatus\" operation for \"" + path +
        "\" on filesystem \"" + fs.getUri() + "\". ";
    String messageExexEx = "Execution exception in \"FileSystem.getFileStatus\" operation for \"" + path +
        "\" on filesystem \"" + fs.getUri() + "\". ";

    // run specified task with retries (we only make retries on timed out tasks):
    BridgeOpStatus status = run(callable, result, RETRIES, messageInterruptedEx, messageExexEx, RETRIES);
    LOG.info("\"FileSystem.getFileStatus\" finished with status \"" + status + "\" for Path \"" + path +
        "\" on fs \"" + fs.getUri() + "\".");
    return result.get();
  }

  /**
   * Execute FileSystem.getFileBlockLocations(FileStatus file, long start, long len) in the controlled way
   *
   * @param fs    - hadoop FileSystem
   * @param file  - hadoop FileStatus
   * @param start - offest within the file
   * @param len   - length to explore for blocks
   *
   * @return compound result, if status is OK, contains the list of block locations found within the file
   * starting from offset "start" and up to len length
   */
  public static BridgeOpResult<BlockLocation[]> getFileBlockLocations(final FileSystem fs, final FileStatus file,
      final long start, final long len){
    AtomicReference<BridgeOpResult<BlockLocation[]>> result = new AtomicReference<BridgeOpResult<BlockLocation[]>>();

    //declaration of the anonymous class
    InterruptableCallable<BlockLocation[]> callable = new InterruptableCallable<BlockLocation[]>() {
      @Override
      protected BlockLocation[] dowork() throws IOException{
        return fs.getFileBlockLocations(file, start, len);
      }
    };

    String messageInterruptedEx = "Timeout exception in \"FileSystem.getFileBlockLocations\" operation for \"" + file.getPath() +
        "\" on filesystem \"" + fs.getUri() + "\". ";
    String messageExexEx = "Execution exception in \"FileSystem.getFileBlockLocations\" operation for \"" + file.getPath() +
        "\" on filesystem \"" + fs.getUri() + "\". ";

    // run specified task with retries (we only make retries on timed out tasks):
    BridgeOpStatus status = run(callable, result, RETRIES, messageInterruptedEx, messageExexEx, RETRIES);
    LOG.info("\"FileSystem.getFileBlockLocations\" finished with status \"" + status + "\" for Path \"" + file.getPath() +
        "\" on fs \"" + fs.getUri() + "\".");
    return result.get();
  }

  /**
   * Execute DistributedFileSystem.getFileBlockStorageLocations(List<BlockLocation> blocks) in the controlled way
   *
   * @param dfs    - hadoop DistributedFileSystem
   * @param blocks - list of hadoop BlockLocation
   *
   * @return compound result, if status is OK, contains list of block storage locations for specified set of blocks
   */
  public static BridgeOpResult<BlockStorageLocation[]> getFileBlockStorageLocations(final DistributedFileSystem dfs,
      final List<BlockLocation> blocks){
    AtomicReference<BridgeOpResult<BlockStorageLocation[]>> result = new AtomicReference<BridgeOpResult<BlockStorageLocation[]>>();

    //declaration of the anonymous class
    InterruptableCallable<BlockStorageLocation[]> callable = new InterruptableCallable<BlockStorageLocation[]>() {
      @Override
      protected BlockStorageLocation[] dowork() throws IOException{
        return dfs.getFileBlockStorageLocations(blocks);
      }
    };

    String messageInterruptedEx = "Timeout exception in \"DistributedFileSystem.getFileBlockStorageLocations\" operation for"
        + " filesystem \"" + dfs.getUri() + "\". ";
    String messageExexEx = "Execution exception in \"DistributedFileSystem.getFileBlockStorageLocations\" operation for "
        + " filesystem \"" + dfs.getUri() + "\". ";

    // run specified task with retries (we only make retries on timed out tasks):
    BridgeOpStatus status = run(callable, result, RETRIES, messageInterruptedEx, messageExexEx, RETRIES);
    LOG.info("\"DistributedFileSystem.getFileBlockStorageLocations\" finished with status \"" + status +
        "\" for fs \"" + dfs.getUri() + "\".");
    return result.get();
  }
}

