/*
 * @file command-exec-state.h
 *
 * @brief Defines the single command execution state. For monitoring
 *
 * @date Feb 10, 2015
 * @author elenav
 */

#ifndef COMMAND_EXEC_STATE_H_
#define COMMAND_EXEC_STATE_H_

#include <boost/bind.hpp>
#include <boost/thread/mutex.hpp>

#include "common/status.h"
#include "runtime/client-cache.h"
#include "runtime/command-executor.h"
#include "service/command-mgr.h"

namespace impala {

/** Defines execution state of a single command */
class CommandMgr::CommandExecState {
 public:
	CommandExecState(const TCommandInstanceCtx& command_instance_ctx,
      ExecEnv* exec_env)
    : command_instance_ctx_(command_instance_ctx),
      executor_(exec_env, boost::bind<void>(
          boost::mem_fn(&CommandMgr::CommandExecState::ReportStatusCb),
              this, _1, _2, _3)),
      client_cache_(exec_env->impalad_client_cache()) {
  }

  /** Calling the d'tor releases all memory and closes all data streams held by executor_. */
  ~CommandExecState() { }

  /**  Call Prepare() for prerequisites, validation, etc. */
  Status Prepare(const TExecRemoteCommandParams& exec_params);

  /** Main loop of command execution. Blocks until execution finishes. */
  void Exec();

  /** getter for wrapped command instance id */
  const TUniqueId& command_instance_id() const {
    return command_instance_ctx_.command_instance_id;
  }

  /** getter for bound coordinator address, to route the callback on completion */
  const TNetworkAddress& coord_address() const {
    return command_instance_ctx_.coord_address;
  }

  /** Set the execution thread, taking ownership of the object */
  void set_exec_thread(Thread* exec_thread) { exec_thread_.reset(exec_thread); }

 private:

  TCommandInstanceCtx  command_instance_ctx_;       /**< wrapped command instance */
  CommandExecutor      executor_;                   /**< command executor, runtime resident. Should be injected with execution environment */

  ImpalaInternalServiceClientCache* client_cache_;  /**< clients cache reference, is coming from execution environment */
  TExecRemoteCommandParams          exec_params_;   /**< command execution parameters */

  boost::scoped_ptr<Thread> exec_thread_;           /**< the thread executing this command */
  boost::mutex              status_lock_;           /**< protects exec_status_ */

  /** set in ReportStatusCb();
   *  If set to <> OK, execution has terminated w/ an error */
  Status exec_status_;

  /** Callback for executor; updates exec_status_ if 'status' indicates an error
   * or if there was a protocol/communication error. */
  void ReportStatusCb(const Status& status, RuntimeProfile* profile, bool done);

  /** Update exec_status_ w/ status, if the former isn't already an error.
   *  Returns current exec_status_. */
  Status UpdateStatus(const Status& status);
};

}



#endif /* COMMAND_EXEC_STATE_H_ */
