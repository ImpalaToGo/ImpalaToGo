/*
 * @file command-executor.cc
 *
 * @date   Feb 24, 2015
 * @author elenav
 */

#include <map>
#include "runtime/command-executor.h"

#include <thrift/protocol/TDebugProtocol.h>
#include <gutil/strings/substitute.h>
#include "util/pretty-printer.h"

#include "common/logging.h"

DEFINE_int32(c_status_report_interval, 5, "interval between profile reports; in seconds");

namespace impala{

CommandExecutor::CommandExecutor(ExecEnv* exec_env,
    const ReportStatusCallback& report_status_cb) :
		exec_env_(exec_env), command_(NULL), obj_pool_(new ObjectPool()), profile_(obj_pool_.get(), "CommandExec_Profile"),
		report_status_cb_(report_status_cb),
		report_thread_active_(false), validated_(false), closed_(false),
		has_thread_token_(false) {
}

CommandExecutor::~CommandExecutor() {
  close();
  // at this point, the report thread should have been stopped
  DCHECK(!report_thread_active_);
}

Status CommandExecutor::validate(const TExecRemoteCommandParams& request) {
	// start the command profiling
	command_sw_.Start();

	has_thread_token_ = true;

	// read command parameters
	const TRemoteShortCommand& params = request.command;

	VLOG_QUERY << "validate(): command instance id = \""
			<< PrintId(request.command_instance_ctx.command_instance_id) << "\".";
	LOG(INFO) << "params : \n" << apache::thrift::ThriftDebugString(params);

	// total_time_counter() is in the runtime_state_ so start it up now.
	SCOPED_TIMER(profile()->total_time_counter());

	// preserve command and its id (for monitoring):
	DCHECK(request.__isset.command_instance_ctx);

	command_id_ = request.command_instance_ctx.command_instance_id;
	validated_ = true;
	return validateInternal(request);
}

Status CommandExecutor::validateInternal(const TExecRemoteCommandParams& request){
	switch(request.command.type){
	case TRemoteShortCommandType::RENAME:
		command_ = new RenameCmdDescriptor(request.command);
        break;
	case TRemoteShortCommandType::DELETE:
		command_ = new DeleteCmdDescriptor(request.command);
		break;
	default:
		command_ = NULL;
		break;
	}
	return (command_ != NULL) ? (command_->validate(request.command) ? Status::OK : Status::CANCELLED) :
			Status::CANCELLED;
}

Status CommandExecutor::runInternal(){
	SCOPED_TIMER(profile()->total_time_counter());
	command_->run();

	// go to completion for profile collection
	complete();
	return Status::OK;
}

Status CommandExecutor::run() {
  LOG(INFO) << "run(): command instance_id = \"" << PrintId(command_id_) << "\".";
  // we need to start the profile-reporting thread before calling run(), since it may block
  if (!report_status_cb_.empty() && FLAGS_c_status_report_interval > 0) {
    boost::unique_lock<boost::mutex> l(report_thread_lock_);
    report_thread_.reset(
        new Thread("command-executor", "report-profile",
            &CommandExecutor::reportProfile, this));
    // make sure the thread started up, otherwise reportProfile() might get into a race
    // with stopReportThread()
    report_thread_started_cv_.wait(l);
    report_thread_active_ = true;
  }
  LOG(INFO) << "run(): reporting thread is started for command instance_id = \"" << PrintId(command_id_) << "\".";
  Status status = runInternal();
  if (!status.ok() && !status.IsCancelled() && !status.IsMemLimitExceeded()) {
	  // Log error message in addition to returning in Status. Some requests
	  // may not receive the message directly and can only retrieve the log.
    LOG(ERROR) << "run() : an error occur : \"" << status.GetErrorMsg() << "\".";
  }
  updateStatus(status);
  return status;
}

void CommandExecutor::reportProfile() {
  LOG(INFO) << "reportProfile(): command instance_id = \"" << PrintId(command_id_) << "\".";
  DCHECK(!report_status_cb_.empty());
  boost::unique_lock<boost::mutex> l(report_thread_lock_);
  // tell open() that we started
  report_thread_started_cv_.notify_one();
  LOG(INFO) << "reportProfile(): thread started for command instance_id = \"" << PrintId(command_id_) << "\".";

  // Jitter the reporting time of remote command by a random amount between
  // 0 and the report_interval.  This way, the coordinator doesn't get all the
  // updates at once so its better for contention as well as smoother progress
  // reporting.
  int report_cmd_offset = rand() % FLAGS_c_status_report_interval;
  boost::system_time timeout = boost::get_system_time()
      + boost::posix_time::seconds(report_cmd_offset);
  // We don't want to wait longer than it takes to run the command:
  stop_report_thread_cv_.timed_wait(l, timeout);

  while (report_thread_active_) {
    boost::system_time timeout = boost::get_system_time()
        + boost::posix_time::seconds(FLAGS_c_status_report_interval);

    // timed_wait can return because the timeout occurred or the condition variable
    // was signaled.  We can't rely on its return value to distinguish between the
    // two cases (e.g. there is a race here where the wait timed out but before grabbing
    // the lock, the condition variable was signaled).  Instead, we will use an external
    // flag, report_thread_active_, to coordinate this.
    stop_report_thread_cv_.timed_wait(l, timeout);

    if (VLOG_FILE_IS_ON) {
      VLOG_FILE << "Reporting " << (!report_thread_active_ ? "final " : " ")
          << "profile for instance \"" << PrintId(command_id_) << "\".";
      std::stringstream ss;
      profile()->PrettyPrint(&ss);
      VLOG_FILE << ss.str();
    }

    if (!report_thread_active_) break;

    if (completed_report_sent_.Read() == 0) {
    	LOG(INFO) << "reportProfile(): sending complete command report for instance_id = \"" << PrintId(command_id_) << "\".";
      // No complete command report has been sent.
      sendReport(false);
    }
  }

  VLOG_FILE << "exiting reporting thread: instance_id = \"" << PrintId(command_id_) << "\".";
}

void CommandExecutor::sendReport(bool done) {
  if (report_status_cb_.empty()) return;

  Status status;
  {
    boost::lock_guard<boost::mutex> l(status_lock_);
    status = status_;
  }

  // Update counters here like plan fragment executor, for example

  // This will send a report even if we were cancelled.  The coordinator will
  // be waiting for a final report and profile.
  report_status_cb_(status, profile(), done || !status.ok());
}

void CommandExecutor::stopReportThread() {
  if (!report_thread_active_) return;
  {
    boost::lock_guard<boost::mutex> l(report_thread_lock_);
    report_thread_active_ = false;
  }
  stop_report_thread_cv_.notify_one();
  report_thread_->Join();
}

void CommandExecutor::complete() {
	LOG (INFO) << "Command executor completed command \"" << command_->name() << "\".n";

	delete command_;
	// Check the atomic flag. If it is set, then a fragment complete report has already
	// been sent.
	bool send_report = completed_report_sent_.CompareAndSwap(0, 1);

	command_sw_.Stop();
	int64_t cpu_and_wait_time = command_sw_.ElapsedTime();
	command_sw_ = MonotonicStopWatch();
	int64_t cpu_time = cpu_and_wait_time;
	// Timing is not perfect.
	if (cpu_time < 0)
		cpu_time = 0;

	stopReportThread();
	if (send_report)
		sendReport(true);
}

void CommandExecutor::updateStatus(const Status& status) {
  if (status.ok()) return;

  bool send_report = completed_report_sent_.CompareAndSwap(0,1);

  {
    boost::lock_guard<boost::mutex> l(status_lock_);
      status_ = status;
  }

  stopReportThread();
  if (send_report)
	  sendReport(true);
}

void CommandExecutor::cancel(){}

}
