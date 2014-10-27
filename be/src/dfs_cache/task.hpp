/**
 * tasks-definitions.h
 *
 *  @brief Workers pool for Cache needs
 *
 *  @date   Sep 30, 2014
 *  @author elenav
 */

#ifndef TASK_H_
#define TASK_H_

#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>

#include <boost/tuple/tuple.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/posix_time/time_formatters.hpp>
#include <type_traits>

#include <iostream>
#include <ctime>

#include "util/thread-pool.h"
#include "util/runtime-profile.h"
#include "dfs_cache/common-include.hpp"

/**
 * @namespace impala
 */
namespace impala
{

/** represent task priority */
enum requestPriority{
    HIGH,
    LOW,
    NOT_SET,
};

/**
 * @namespace request
 */
namespace request
{

/**
 * Generic Task, supports running via () operator and a callback.
  */
class Task{
protected:
	boost::posix_time::ptime  m_taskCreation;   /**< task creation timestamp */
	taskOverallStatus         m_status;         /**< overall task status. By default, NOT_RUN */

	bool                      m_invalidated;    /**< task invalidation flag. Having this flag, task should not run
	                                             if it was not run yet. */
	/** Implementor should provide completion callback scenario here */
	virtual void callback() = 0;

	virtual ~Task() = default;

	/** Initializes task creation timestamp. */
	Task() : m_status(taskOverallStatus::NOT_RUN), m_invalidated(false) {
		m_taskCreation = boost::posix_time::microsec_clock::local_time();
	}

public:

	/** "do work" predicate */
    virtual void operator()() = 0;

	/** getter for underlying creation timestamp. Result is not an lvalue! */
	inline boost::posix_time::ptime timestamp() const { return m_taskCreation; }

	/** string representation of timestamp - for hashing */
	inline std::string timestampstr() const { return boost::posix_time::to_iso_string(m_taskCreation); }

	/** Getter for task status */
	inline taskOverallStatus status() const { return m_status; }

	/** Setter for task status */
	inline void status(taskOverallStatus status) { m_status = status; }

	/** getter for "invalidated" flag */
	inline void invalidate() { m_invalidated = true; }

	/** invalidation of task triggering, is only works in "invalidation" points that
	 * should be controlled within the main task run scenarios
	 */
	inline bool invalidated() const { return m_invalidated; }

	/** summarize the task status in regards whether te task should be treated as failed */
	inline bool failure() const {
		return (m_status != taskOverallStatus::COMPLETED_OK &&
			m_status != taskOverallStatus::CANCELATION_SENT &&
			m_status != taskOverallStatus::CANCELED_CONFIRMED);
	}

};

}

// namespace impala

/** Describes the thread pool for dfs-related requests */
typedef ThreadPool<boost::shared_ptr<request::Task> > dfsThreadPool;

namespace request {
/**
 * Task that provides the cancellation
 */
class CancellableTask : virtual public Task{
protected:
	/** Cancellation section. For sync cancellation of  related executor */
	boost::condition_variable_any m_controlCancelationCompletionCondition;   /** condition variable, should be shared with the @a m_functor in order
	 	 	 	 	 	 	 	 	 	 	 	                               * to support cancellation by functor */
	mutable boost::mutex          m_controlCancelationCompletionMux;         /** mutex to guard the "condition" flag */
    bool                          m_controlCancelationCompletionFlag;         /** condition which will control the possible cancellation */

    ~CancellableTask() = default;
public:

    CancellableTask() : m_controlCancelationCompletionFlag(false) {  }

	/**
	 * Implementor should provide cancellation scenario here
	 *
	 * @param async - flag, indicates whether async cancel is requested.
	 * If async, no need to wait for cancellation confirmation,
	 * otherwise the calling thread will wait until the cancellation is confirmed.
	 */
	virtual taskOverallStatus cancel(bool async = false) = 0;

	/**
	 * Getter for condition variable that guards the cancellation condition
	 */
    void conditionvar(boost::condition_variable_any*& conditionvar) { conditionvar = &m_controlCancelationCompletionCondition; }

    /**
     * Getter for cancellation condition
     */
    void condition(bool*& condition) { condition = &m_controlCancelationCompletionFlag; }

    /**
     * Getter for cancellation condition
     */
    inline bool condition() const { return m_controlCancelationCompletionFlag; }

    /**
     * Getter for guarding mutex
     */
    void mux(boost::mutex*& mux) { mux = &m_controlCancelationCompletionMux; }
};

/**
 * Task that provides the progress
 */
template<typename Progress_>
class MakeProgressTask : virtual public CancellableTask{
protected:
	Progress_  m_progress;               /**< progress representation */

    virtual ~MakeProgressTask() = default;
public:

    /** get the progress */
	Progress_ progress() { return m_progress; }
};

/**
 * Task bound to calling context and is marked with context to be reffered to later by this context
 */
template<typename Progress_>
class SessionBoundTask : virtual public MakeProgressTask<Progress_>{

protected:
	SessionContext            m_session;        /**< bound client session descriptor */
	bool 					  m_async;          /**< flag, indicates that the task is async */

	dfsThreadPool*            m_pool;                              /**< reference to a thread pool to schedule subtasks */
	boost::condition_variable m_controlDataSetScheduledCondition;  /**< condition variable to have signaling from this task that the task is scheduled */
	boost::mutex              m_controlDataSetScheduledMux;        /**< guard for task "is scheduled" signal */
	bool                      m_controlDataSetScheduledFlag;       /**< flag, indicates that the task is scheduled */

	virtual ~SessionBoundTask() = default;
public:

	/**
	 * Ctor. Created assuming user context.
	 */
    SessionBoundTask(const SessionContext& session, dfsThreadPool* pool, bool async = true) :
    	m_session(session), m_async(async), m_pool(pool), m_controlDataSetScheduledFlag(false) { }

	/** getter for underlying client context */
	SessionContext session() const { return m_session; }

	/** getter for "is task is scheduled" flag */
	bool scheduled() { return m_controlDataSetScheduledFlag; }

	/** Wait until the task will be scheduled */
	template<typename predicate_type>
	void waitScheduled(predicate_type predicate){
		boost::unique_lock<boost::mutex> lock(m_controlDataSetScheduledMux);
		m_controlDataSetScheduledCondition.wait(lock, predicate);
		lock.unlock();
	}

	/** getter for "async" flag describes the running context task nature */
	bool async() { return m_async; }
};

/**
 * Runnable Task.
 * Is described by @a "completion callback", @a "do work functor", @a "cancellation functor".
 * The state which is handled by Runnable Task:
 * - performance statistic measured in context of this Task execution (For monitoring and alarming).
 * - synchronization variables required in cancellation scenario
 */
template<typename CompletionCallback_, typename Functor_, typename Cancellation_, typename Progress_>
class RunnableTask : virtual public MakeProgressTask<Progress_>
{
private:
	MonotonicStopWatch      m_sw;        /**< stop watch to measure the time the task is running */
	int64_t                 m_lifetime;  /**< time which elapsed since the task was started */
	std::clock_t            m_start;     /**< start time when the task was started */
    int64_t                 m_cputime;   /**< time which the task spent on CPU */

protected:
	CompletionCallback_      m_callback;    /** < callback to invoke on caller when Prepare request is
	 	 	 	 	 	 	 	 	 	     * completed or interrupted (whatever status)*/
	Functor_                 m_functor;     /** functor to run the task */

	Cancellation_            m_cancelation; /** functor to perform the task cancellation */

	requestPriority          m_priority;    /** request priority */
	request_performance      m_performance; /** task performance */

    /** Sync stuff */
    bool         		      m_controlDataSetCompletionFlag;
    boost::mutex 			  m_controlDataSetCompletionMux;
    boost::condition_variable m_controlDataSetCompletionCondition;

	/** "Do work" predicate */
	virtual void run_internal() = 0;

    /** "finalize" predicate */
    virtual void finalize() = 0;

	virtual ~RunnableTask() = default;

	int64_t cpu_time(){ return m_cputime = (std::clock() - m_start) / (double)(CLOCKS_PER_SEC / 1000); }

public:
	RunnableTask(CompletionCallback_ callback, Functor_ functor, Cancellation_ cancellation)
		: m_lifetime(0), m_start(std::clock()), m_cputime(0), m_callback(callback),
		  m_functor(functor), m_cancelation(cancellation), m_priority(requestPriority::NOT_SET), m_controlDataSetCompletionFlag(false){}

	/**
	 * query request performance
	 *
	 */
	request_performance performance(){
		// overall request lifetime and CPU calculations
		m_performance.lifetime = m_lifetime = m_sw.ElapsedTime();
		m_performance.cpu_time_miliseconds = cpu_time();

		return m_performance;
	}

	/** getter for request priority */
	requestPriority priority() { return m_priority; }

	/**
	 * Operator overload in order to be callable by boost::asio::io_services threadpool (which we do not use right now :) )
	 * Here're we see all phases of requests execution:
	 * - main routine;
	 * - callback to a client;
	 * - finalization
	 */
	void  operator()(){
		m_sw.Start();

		// run internal request
		run_internal();

		// make measurements
		m_lifetime = m_sw.ElapsedTime();
		cpu_time();

		// run client callback on the same thread
	    this->callback();

	    // make measurements
	    m_lifetime = m_sw.ElapsedTime();
	    m_sw.Stop();
	    cpu_time();

	    // run finalizations
	    this->finalize();
   }
};

/**
 * Some runnable task that is bound to some session context.
 * It should pass through the session context in its completion callback
 */
template<typename CompletionCallback_, typename Functor_, typename Cancellation_, typename Progress_> class ContextBoundTask :
		public RunnableTask<CompletionCallback_, Functor_, Cancellation_, Progress_>, public SessionBoundTask<Progress_>{
protected:
	virtual ~ContextBoundTask() = default;

public:
	ContextBoundTask(CompletionCallback_ callback, Functor_ functor, Cancellation_ cancelation, const SessionContext& session,
		dfsThreadPool* pool, bool async = true ) :
			RunnableTask<CompletionCallback_, Functor_, Cancellation_, Progress_>(callback, functor, cancelation),
			SessionBoundTask<Progress_>(session, pool, async){}
};


} /** request */

} /** impala */

#endif /* TASK_H_ */
