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

#ifndef IMPALA_SERVICE_IMPALA_INTERNAL_SERVICE_H
#define IMPALA_SERVICE_IMPALA_INTERNAL_SERVICE_H

#include <boost/shared_ptr.hpp>

#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "service/impala-server.h"
#include "service/fragment-mgr.h"
#include "service/command-mgr.h"

namespace impala {

/**
 * Proxies Thrift RPC requests onto their implementing objects for the
 * ImpalaInternalService service.
 */
class ImpalaInternalService : public ImpalaInternalServiceIf {
 public:
  ImpalaInternalService(const boost::shared_ptr<ImpalaServer>& impala_server,
      const boost::shared_ptr<FragmentMgr>& fragment_mgr,
	  const boost::shared_ptr<CommandMgr>& command_mgr)
      : impala_server_(impala_server), fragment_mgr_(fragment_mgr), command_mgr_(command_mgr) { }

  virtual void ExecPlanFragment(TExecPlanFragmentResult& return_val,
      const TExecPlanFragmentParams& params) {
    fragment_mgr_->ExecPlanFragment(params).SetTStatus(&return_val);
  }

  virtual void CancelPlanFragment(TCancelPlanFragmentResult& return_val,
      const TCancelPlanFragmentParams& params) {
    fragment_mgr_->CancelPlanFragment(return_val, params);
  }

  virtual void ReportExecStatus(TReportExecStatusResult& return_val,
      const TReportExecStatusParams& params) {
    impala_server_->ReportExecStatus(return_val, params);
  }

  virtual void TransmitData(TTransmitDataResult& return_val,
      const TTransmitDataParams& params) {
    impala_server_->TransmitData(return_val, params);
  }

  /** Execute the short command. Mostly is introduced to execute the remote dfs commands by
   * nodes that are responsible to cache the part of remote dfs content.
   *
   * @param [in/out] return_val - return value, operation synchronous result
   * @param [in]     params     - command parameters
   */
  virtual void ExecShortCommand(TRemoteShortCommandResult& return_val,
		  const TExecRemoteCommandParams& params){
	  command_mgr_->ExecCommand(params).SetTStatus(&return_val);
  }

  /** Polling method to request the command execution triggered by preseding
   * ExecShortCommand() RPC.
   *
   * @param [in/out] return_val - command status
   * @param [in]     params     - monitoring request parameters
   */
  virtual void ReportCommandStatus(TReportCommandStatusResult& return_val,
		  const TReportCommandStatusParams& params){

  }
 private:
  // Manages fragment reporting and data transmission
  boost::shared_ptr<ImpalaServer> impala_server_;

  // Manages fragment execution
  boost::shared_ptr<FragmentMgr> fragment_mgr_;

  // Manages command execution
  boost::shared_ptr<CommandMgr> command_mgr_;
};

}

#endif
