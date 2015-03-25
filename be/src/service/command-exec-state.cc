/*
 * command-exec-state.cc
 *
 *  Created on: Feb 10, 2015
 *      Author: elenav
 */

#include "service/command-exec-state.h"

#include <sstream>

#include "codegen/llvm-codegen.h"
#include "rpc/thrift-util.h"

namespace impala{
Status CommandMgr::CommandExecState::UpdateStatus(const Status& status) {
  boost::lock_guard<boost::mutex> l(status_lock_);
  if (!status.ok() && exec_status_.ok()) exec_status_ = status;
  return exec_status_;
}

Status CommandMgr::CommandExecState::Prepare(const TExecRemoteCommandParams& exec_params) {
  exec_params_ = exec_params;
  RETURN_IF_ERROR(executor_.validate(exec_params));
  return Status::OK;
}

void CommandMgr::CommandExecState::Exec() {
  executor_.run();
  executor_.close();
}

/**
 * There can only be one of these callbacks in-flight at any moment, because
 * it is only invoked from the executor's reporting thread.
 * Also, the reported status will always reflect the most recent execution status,
 * including the final status when execution finishes.
 */
void CommandMgr::CommandExecState::ReportStatusCb(const Status& status,
		RuntimeProfile* profile, bool done) {
  DCHECK(status.ok() || done);  // if !status.ok() => done
  Status exec_status = UpdateStatus(status);

  LOG(INFO) << "Command Mgr is near to update on command exec status..\n";

  Status coord_status;
  ImpalaInternalServiceConnection coord(client_cache_, coord_address(), &coord_status);
  if (!coord_status.ok()) {
    std::stringstream s;
    s << "couldn't get a client for " << coord_address();
    UpdateStatus(Status(TStatusCode::INTERNAL_ERROR, s.str()));
    return;
  }

  TReportCommandStatusParams params;
  params.protocol_version = ImpalaInternalServiceVersion::V1;
  params.__set_backend_num(command_instance_ctx_.backend_num);
  params.__set_command_instance_id(command_instance_ctx_.command_instance_id);
  params.__set_query_id(command_instance_ctx_.query_id);
  exec_status.SetTStatus(&params);
  params.__set_done(done);
  profile->ToThrift(&params.profile);
  params.__isset.profile = true;
  params.__isset.error_log = (params.error_log.size() > 0);

  TReportCommandStatusResult res;
  Status rpc_status;
  try {
    try {
    	// reply back to cordinator the execution status
      coord->ReportCommandStatus(res, params);
    } catch (const apache::thrift::TException& e) {
      VLOG_RPC << "Retrying ReportExecStatus: " << e.what();
      rpc_status = coord.Reopen();
      if (!rpc_status.ok()) {
        // we need to cancel the execution of this command
        UpdateStatus(rpc_status);
        executor_.cancel();
        return;
      }
      coord->ReportCommandStatus(res, params);
    }
    rpc_status = Status(res.status);
  } catch (apache::thrift::TException& e) {
    std::stringstream msg;
    msg << "ReportExecStatus() to " << coord_address() << " failed:\n" << e.what();
    VLOG_QUERY << msg.str();
    rpc_status = Status(TStatusCode::INTERNAL_ERROR, msg.str());
  }

  if (!rpc_status.ok()) {
    // we need to cancel the execution of this command
    UpdateStatus(rpc_status);
    executor_.cancel();
  }
}

}
