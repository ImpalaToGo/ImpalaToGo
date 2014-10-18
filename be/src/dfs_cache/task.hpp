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

#include "dfs_cache/common-include.hpp"

/**
 * @namespace impala
 */
namespace impala
{

/**
 * Any task overall status
 */
enum class taskOverallStatus
{
	NOT_RUN      = 0,
	IN_PROGRESS  = 1,
    COMPLETED_OK = 2,
    FAILURE      = 3,
    CANCELATION_SENT   = 4,
    CANCELED_CONFIRMED = 5,  /**< task cancellation was performed successfully */
    NOT_FOUND          = 6,  /**< task not found */
    IS_NOT_MANAGED     = 7,  /**< task is not managed */

};

/**
 * @namespace request
 */
namespace request
{

class Task{
protected:
std::string testm;
	virtual ~Task() = default;

public:
	/**
	 * Implementor should provide completion callback scenario here
	 */
	virtual void callback() = 0;

    virtual void operator()() = 0;
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
class MakeProgressTask : public virtual CancellableTask{
protected:
	boost::shared_ptr<Progress_>  m_progress;               /**< progress representation */
    virtual ~MakeProgressTask() = default;
public:

    MakeProgressTask() {}

    /** get the progress */
	boost::shared_ptr<Progress_> progress() { return m_progress; }
};

class SessionBoundTask : virtual public CancellableTask{

protected:
	virtual ~SessionBoundTask() = default;
public:
	SessionContext           m_session;     /**< bound client session descriptor */
	SessionContext session() { return m_session; }
    SessionBoundTask(SessionContext session) : m_session(session){}
};

/**
 * Runnable Task.
 * Is described by @a "completion callback", @a "do work functor", @a "cancellation functor".
 * The state which is handled by Runnable Task:
 * - performance statistic measured in context of this Task execution (For monitoring and alarming).
 * - synchronization variables required in cancellation scenario
 */
template<typename CompletionCallback_, typename Functor_, typename Cancellation_, typename Progress_>
class RunnableTask : public MakeProgressTask<Progress_>
{
private:
	boost::timer::cpu_timer m_timer;     /** timer to track CPU time */
	boost::timer::cpu_times m_cputime;   /** CPU time takes to run the request */

protected:
	CompletionCallback_      m_callback;    /** < callback to invoke on caller when Prepare request is
	 	 	 	 	 	 	 	 	 	     * completed or interrupted (whatever status)*/
	Functor_                 m_functor;     /** functor to run the task */

	Cancellation_            m_cancelation; /** functor to perform the task cancellation */

	taskOverallStatus        m_status;      /**< overall task status. By default, NOT_RUN */

	request_performance      m_performance; /** task performance */
	/**
	 * Implementor should provide run scenario here
	 */
	virtual taskOverallStatus run_internal() = 0;

	virtual ~RunnableTask() = default;

public:
	RunnableTask(CompletionCallback_ callback, Functor_ functor, Cancellation_ cancellation)
		: m_callback(callback), m_functor(functor), m_cancelation(cancellation),
		  m_status(taskOverallStatus::NOT_RUN){}

	/** Getter for task status */
	taskOverallStatus status() { return m_status; }

	/**
	 * query request performance
	 *
	 */
	request_performance performance(){
		m_performance.cpu_time = m_cputime = m_timer.elapsed(); // does not stop, safe to proceed the metric
		return m_performance;
	}

	/**
	 * Operator overload in order to be callable by boost::asio::io_services
	 */
	void  operator()(){
		m_timer.start();
		run_internal();
		m_cputime = m_timer.elapsed();
		this->callback();
   }
};

/**
 * Some runnable task that is bound to some session context.
 * It should pass through the session context in its completion callback
 */
template<typename CompletionCallback_, typename Functor_, typename Cancellation_, typename Progress_> class ContextBoundTask :
		public RunnableTask<CompletionCallback_, Functor_, Cancellation_, Progress_>, public SessionBoundTask{
protected:
	virtual ~ContextBoundTask() = default;

public:
	ContextBoundTask(CompletionCallback_ callback, Functor_ functor, Cancellation_ cancelation, SessionContext session ) :
		RunnableTask<CompletionCallback_, Functor_, Cancellation_, Progress_>(callback, functor, cancelation), SessionBoundTask(session){}
};


} /** request */
} /** impala */

#endif /* TASKS_DEFINITIONS_H_ */
