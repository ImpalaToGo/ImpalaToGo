/*
 * @file command-executor.h
 * @brief command executor binds together:
 * - execution context - initial parameters and current execution status;
 * - execution environment;
 * - runtime profile (performance monitoring statistics);
 * - completion/status invocation callbacks
 *
 * @date   Feb 24, 2015
 * @author elenav
 */

#ifndef COMMAND_EXECUTOR_H_
#define COMMAND_EXECUTOR_H_

// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef IMPALA_SERVICE_PLAN_EXECUTOR_H
#define IMPALA_SERVICE_PLAN_EXECUTOR_H

#include <vector>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/function.hpp>

#include "runtime/exec-env.h"
#include "runtime/descriptors-command.h"
#include "common/status.h"
#include "common/object-pool.h"
#include "util/runtime-profile.h"
#include "util/thread.h"

namespace impala {

class HdfsFsCache;
class RuntimeProfile;
class TRemoteShortCommand;
class TExecRemoteCommandParams;
class TUniqueId;

/** CommandExecutor handles all aspects of the execution of a command,
 *  including setup and tear-down, both in the success and error case.
 *  Tear-down frees all memory allocated for this command if any,
 *  and releases all resources were utilized in context of this command
 *  if any; it happens automatically in the d'tor.
 *
 *  The executor makes the profile for the command available,
 *  which includes profile information for the command itself.
 *
 *  The ReportStatusCallback passed into the c'tor is invoked periodically to report the
 *  the command execution status. The frequency of those reports is controlled by the flag
 *  status_report_interval; setting that flag to 0 disables periodic reporting altogether.
 *
 *  Regardless of the value of that flag, if a report callback is specified, it is
 *  invoked at least once at the end of execution with an overall status and profile
 *  (and 'done' indicator). The only exception is when execution is cancelled, in which
 *  case the callback is *not* invoked (the coordinator already knows that execution
 *  stopped, because it initiated the cancellation).
 *  Aside from cancel(), which may be called asynchronously (TODO), this class is not thread-safe.
 */
class CommandExecutor {
 public:
  /**
   * Callback to report execution status of a command.
   * 'profile' is the cumulative profile (TODO), 'done' indicates whether the execution
   * is done or still continuing.
  */
  typedef boost::function<
      void (const Status& status, RuntimeProfile* profile, bool done)>
      ReportStatusCallback;

  /** report_status_cb, if !empty(), is used to report the accumulated profile
   * information periodically during execution (Open() or GetNext()).
   */
  CommandExecutor(ExecEnv* exec_env, const ReportStatusCallback& report_status_cb);

  /**
   * Closes the underlying command and frees up all resources allocated in Open().
   * It is an error to delete a CommandExecutor with a report callback
   * before Open() indicated that execution is finished.
   */
  ~CommandExecutor();

  /**
   * Validate command execution. Call this prior to run().
   * This call won't block.
   * runtime_state() will not be valid until validate() is called.
   */
  Status validate(const TExecRemoteCommandParams& request);

  /** internal command parameters validation, according to particular command specifics */
  Status validateInternal(const TExecRemoteCommandParams& request);

  /** Start execution */
  Status run();

  /** internal run, according to specifics of particular command*/
  Status runInternal();

  /** Closes the underlying command and frees up all resources allocated in run() - if any */
  void close() {}

  /** TODO : cancellation is not implemented as not required yet */
  void cancel();

  /** Profile information for executed command */
  RuntimeProfile* profile() { return &profile_; }

 private:
  ExecEnv*              exec_env_;  	 /**< reference to exec environment */
  CommandDescriptor*    command_;        /**< command to execute */
  TUniqueId             command_id_;     /**< command unique id */

  /******************************* Monitoring section below    ********************************************************/
  boost::scoped_ptr<ObjectPool> obj_pool_;

  RuntimeProfile       profile_;                  /**< runtime profile for the command to be executed */
  ReportStatusCallback report_status_cb_;         /**< report status callback. For updates on overall status, profiling
  	  	  	  	  	  	  	  	  	  	  	  	   * counters if any, and the completion marker */
  boost::scoped_ptr<Thread> report_thread_;       /**< reporting thread, for sampling perf counters if any configured */
  boost::mutex              report_thread_lock_;  /**< guard to protect the take-and-report the sample moment */

  /** Indicates that profile reporting thread should stop. Tied to report_thread_lock_. */
  boost::condition_variable stop_report_thread_cv_;

  /** Indicates that profile reporting thread started. Tied to report_thread_lock_. */
  boost::condition_variable report_thread_started_cv_;

  bool report_thread_active_;  /**< flag, indicates that reporting thread is started (if set) */

  /*******************************************************************************************************************/

  bool validated_; 			   /**< flag, indicates that validation - validate() - was run and completed with "OK".
  	  	  	  	  	  	  	      * Being set, day the executor is ready to go */
  bool closed_;    			   /**< flag, indicates that the command execution is closed, not active */

  /** true if this command still owns the thread token, has not returned the thread token to the thread resource mgr */
  bool has_thread_token_;

  /** Overall execution status. Either ok() or set to the first error status that was encountered. */
  Status status_;

  // Protects status_
  // lock ordering:
  // 1. report_thread_lock_
  // 2. status_lock_
  boost::mutex status_lock_;

  // Stopwatch for this command execution. Started in validate(), stopped in close().
  MonotonicStopWatch command_sw_;

  /** (Atomic) Flag, indicates whether a completed command report has been or is going to be fired.
   * It is initialized to 0 and atomically swapped to 1 when a completed command report is about to be fired.
   * Used in order to reduce the probability that the particular report is being sent twice at the end of the command.
   */
  AtomicInt<int> completed_report_sent_;

  /** Main loop of profile reporting thread. Exits when notified on done_cv_.
   * On exit, no report is being sent, i.e., this will not send the final report.
   */
  void reportProfile();

  /** Invoke the report callback if there is a report callback configured and the current
   * status isn't CANCELLED. Set 'done' in the callback invocation if
   * (done == true) or we have an error status.
   */
  void sendReport(bool done);

  /** If status_.ok(), sets status_ to status. If we're transitioning to an error status,
   * stops report thread and send the final report.
   */
  void updateStatus(const Status& status);

  /** To be invoked when the command execution is complete to finalize counters if any. */
  void complete();

  /** Stops report thread, if one is running. Blocks until report thread terminates. */
  void stopReportThread();
};

}

#endif




#endif /* COMMAND_EXECUTOR_H_ */
