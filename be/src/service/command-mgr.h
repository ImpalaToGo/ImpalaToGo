/*
 * @file command-manager.h
 *
 * @brief Command Manager is responsible in general to run the requested command in separate thread and hold the execution
 * context for all managed commands until they are done.
 *
 * @date   Feb 10, 2015
 * @author elenav
 */

#ifndef COMMAND_MGR_H_
#define COMMAND_MGR_H_

#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/unordered_map.hpp>

#include "gen-cpp/ImpalaInternalService.h"
#include "common/status.h"
#include "runtime/exec-env.h"
#include "runtime/thread-resource-mgr.h"

namespace impala {

/* Manages execution of individual commands, which are typically run as a result
* ExecShortCommand RPCs that arrive via the internal Impala interface.
*
* The command is being executed in ExecCommand(), those, in turn, starts a thread for execution.
* The command execution may be either cancelled via CancelCommandExecution() - TODO - or completed
* with a status.
*/
class CommandMgr {
 public:
  /**
   * Executes the command specified
   *
   * @param params - command parameters
   *
   * @return execution status
   */
  Status ExecCommand(const TExecRemoteCommandParams& params);

  ~CommandMgr();

  /** initialization for Command manager. basically to allocate working resources */
  Status init();

  /** getter for resource pool */
  ThreadResourceMgr::ResourcePool* resource_pool() { return resource_pool_; }

 private:
  class CommandExecState;

  /**
   * Call exec_state->Exec(), and then removes exec_state from the command map. Run in
   * the command's execution thread.
   */
  void CommandExecThread(CommandExecState* exec_state);

  /** Getter for particular command execution state
   *
   *  @param command_instance_id - unique identificator for the command to query the status on
   *
   *  @return command execution status
   */
  boost::shared_ptr<CommandExecState> GetCommandExecState(
      const TUniqueId& command_instance_id);

  /** protects commands_exec_state_map_ */
  boost::mutex commands_exec_state_map_lock_;

  /**
   * Map from command id to exec state; CommandExecState is owned by us and
   * referenced as a shared_ptr to allow asynchronous calls to CancelCommandExecution()
   */
  typedef boost::unordered_map<TUniqueId, boost::shared_ptr<CommandExecState> > CommandExecStateMap;
  CommandExecStateMap commands_exec_state_map_;

  /** Thread resource management object for this fragment's execution.  The runtime
   * state is responsible for returning this pool to the thread mgr.
   */
  ThreadResourceMgr::ResourcePool* resource_pool_;

};

}



#endif /* COMMAND_MANAGER_H_ */
