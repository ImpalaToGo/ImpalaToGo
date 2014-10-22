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
#include <boost/tuple/tuple.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/posix_time/time_formatters.hpp>
#include <type_traits>

#include <iostream>
#include <ctime>

#include "util/runtime-profile.h"
#include "dfs_cache/common-include.hpp"

/**
 * @namespace impala
 */
namespace impala
{
/**
 * Any task overall status
 */
enum taskOverallStatus{
            NOT_RUN = 0,
            PENDING,
            IN_PROGRESS,
            COMPLETED_OK,
            FAILURE,
            CANCELATION_SENT,
            CANCELED_CONFIRMED,    /**< task cancellation was performed successfully */
            NOT_FOUND,             /**< task not found */
            IS_NOT_MANAGED      /**< task is not managed */
};

extern std::ostream& operator<<(std::ostream& out, const taskOverallStatus value);

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
	taskOverallStatus         m_status;          /**< overall task status. By default, NOT_RUN */

	virtual ~Task() = default;

	/** Initializes task creation timestamp. */
	Task() : m_status(taskOverallStatus::NOT_RUN) {
		m_taskCreation = boost::posix_time::microsec_clock::local_time();
	}

public:
	/**
	 * Implementor should provide completion callback scenario here
	 */
	virtual void callback() = 0;

    virtual void operator()() = 0;

	/** getter for underlying creation timestamp */
	boost::posix_time::ptime timestamp() const { return m_taskCreation; }

	/** string representation of timestamp - for hashing */
	std::string timestampstr() const { return boost::posix_time::to_iso_string(m_taskCreation); }

	/** Getter for task status */
	taskOverallStatus status() const { return m_status; }
};

/**
 * Task that provides the cancellation
 */
class CancellableTask : virtual public Task{
private:
	/** Cancellation section. For sync cancellation of  related executor */
	boost::condition_variable_any m_conditionvar;   /** condition variable, should be shared with the @a m_functor in order
	 	 	 	 	 	 	 	 	 	 	 	                      * to support cancellation by functor
	                                                                  */
	mutable boost::mutex          m_mux;            /** mutex to lock the shared data */

protected:
    bool                          m_condition;      /** condition which will control the possible cancellation */
    ~CancellableTask() = default;
public:

    CancellableTask() : m_condition(false) {}

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
    void conditionvar(boost::condition_variable_any*& conditionvar) { conditionvar = &m_conditionvar; }

    /**
     * Getter for cancellation condition
     */
    void condition(bool*& condition) { condition = &m_condition; }

    /**
     * Getter for cancellation condition
     */
    bool condition() { return m_condition; }

    /**
     * Getter for guarding mutex
     */
    void mux(boost::mutex*& mux) { mux = &m_mux; }
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

	virtual ~SessionBoundTask() = default;
public:

	/** getter for underlying client context */
	SessionContext session() const { return m_session; }

	/**
	 * Ctor. Created assuming user context.
	 */
    SessionBoundTask(const SessionContext& session) : m_session(session){

    }
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

	request_performance      m_performance; /** task performance */
	/**
	 * Implementor should provide run scenario here
	 */
	virtual taskOverallStatus run_internal() = 0;

	virtual ~RunnableTask() = default;

	int64_t cpu_time(){ return m_cputime = (std::clock() - m_start) / (double)(CLOCKS_PER_SEC / 1000); }

public:
	RunnableTask(CompletionCallback_ callback, Functor_ functor, Cancellation_ cancellation)
		: m_callback(callback), m_functor(functor), m_cancelation(cancellation), m_lifetime(0), m_cputime(0), m_start(std::clock()){}

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

	/**
	 * Operator overload in order to be callable by boost::asio::io_services threadpool (which we do not use right now :) )
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
	ContextBoundTask(CompletionCallback_ callback, Functor_ functor, Cancellation_ cancelation, const SessionContext& session ) :
		RunnableTask<CompletionCallback_, Functor_, Cancellation_, Progress_>(callback, functor, cancelation), SessionBoundTask<Progress_>(session){}
};


} /** request */
} /** impala */

#endif /* TASK_H_ */
