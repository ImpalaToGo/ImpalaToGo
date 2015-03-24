/*
 * command-mgr.cc
 *
 *  Created on: Feb 10, 2015
 *      Author: elenav
 */

#include "service/command-mgr.h"

#include <boost/lexical_cast.hpp>
#include <google/malloc_extension.h>
#include <gutil/strings/substitute.h>

#include "service/command-exec-state.h"
#include "runtime/exec-env.h"
#include "util/impalad-metrics.h"
#include "util/uid-util.h"

namespace impala {

DEFINE_int32(log_c_mem_usage_interval, 0,
		"If non-zero, impalad will output memory usage "
				"every log_c_mem_usage_interval'th command completion.");

Status CommandMgr::init(){
	LOG (INFO) << "Command manager is going to register its pool within the system." << "\n";
	// acquire resources (here, threads) pool from exec, typically 3 number of threads per core
	resource_pool_ = ExecEnv::GetInstance()->thread_mgr()->RegisterPool();
	LOG (INFO) << "Command manager completed pool registration within the system." << "\n";

	DCHECK(resource_pool_ != NULL);
	LOG (INFO) << "Command manager is initialized." << "\n";
	return Status::OK;
}

CommandMgr::~CommandMgr(){
	LOG(INFO) << "Shutting down Command Manager... Unregistering the resources pool within the system...";
	ExecEnv::GetInstance()->thread_mgr()->UnregisterPool(resource_pool());
}

Status CommandMgr::ExecCommand(const TExecRemoteCommandParams& params){
	VLOG_QUERY << "ExecCommand() command_instance_id="
						<< params.command_instance_ctx.command_instance_id
						<< " coord="
						<< params.command_instance_ctx.coord_address
						<< " backend#="
						<< params.command_instance_ctx.backend_num;

	boost::shared_ptr<CommandExecState> exec_state(
			new CommandExecState(params.command_instance_ctx,
					ExecEnv::GetInstance()));

	LOG(INFO) << "Command exec state is created...";

    RETURN_IF_ERROR(exec_state->Prepare(params));

	boost::lock_guard<boost::mutex> l(commands_exec_state_map_lock_);
		// register exec_state before starting exec thread
	commands_exec_state_map_.insert(
				std::make_pair(params.command_instance_ctx.command_instance_id,
						exec_state));

	// Reserve one main thread from the pool
	if(resource_pool() != NULL)
		resource_pool()->AcquireThreadToken();

	exec_state->set_exec_thread(
			new Thread("impala-server", "exec-command",
					&CommandMgr::CommandExecThread, this, exec_state.get()));

	return Status::OK;
}

void CommandMgr::CommandExecThread(CommandExecState* exec_state) {
	LOG(INFO) << "New command is received for execution. \n";

	ImpaladMetrics::IMPALA_SERVER_NUM_COMMANDS->Increment(1L);
	exec_state->Exec();
	if(resource_pool() != NULL)
		// we're done with this command
		resource_pool()->ReleaseThreadToken(true);

	// The last reference to the CommandExecState is in the map. We don't
	// want the destructor to be called while the command_exec_state_map_lock_
	// is taken so we'll first grab a reference here before removing the entry
	// from the map.
	boost::shared_ptr<CommandExecState> exec_state_reference;
	{
		boost::lock_guard<boost::mutex> l(commands_exec_state_map_lock_);
		CommandExecStateMap::iterator i = commands_exec_state_map_.find(
				exec_state->command_instance_id());
		if (i != commands_exec_state_map_.end()) {
			exec_state_reference = i->second;
			commands_exec_state_map_.erase(i);
		} else {
			LOG(ERROR)<< "missing entry in command exec state map: instance_id="
			<< exec_state->command_instance_id();
		}
	}
#ifndef ADDRESS_SANITIZER
	// tcmalloc and address sanitizer can not be used together
	if (FLAGS_log_c_mem_usage_interval > 0) {
		uint64_t num_complete =
				ImpaladMetrics::IMPALA_SERVER_NUM_COMMANDS->value();
		if (num_complete % FLAGS_log_c_mem_usage_interval == 0) {
			char buf[2048];
			// This outputs how much memory is currently being used by this impalad
			MallocExtension::instance()->GetStats(buf, 2048);
			LOG(INFO)<< buf;
		}
	}
#endif
}

boost::shared_ptr<CommandMgr::CommandExecState> CommandMgr::GetCommandExecState(
		const TUniqueId& command_instance_id) {
	boost::lock_guard<boost::mutex> l(commands_exec_state_map_lock_);
	CommandExecStateMap::iterator i = commands_exec_state_map_.find(command_instance_id);
	if (i == commands_exec_state_map_.end())
		return boost::shared_ptr<CommandExecState>();
	else
		return i->second;
}
}

