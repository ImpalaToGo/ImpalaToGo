/**
 * @file cache-work-pool.h
 * @brief Workers pool for Cache needs
 *
 * @date   Sep 30, 2014
 * @author elenav
 */

#ifndef CACHE_WORK_POOL_H_
#define CACHE_WORK_POOL_H_

#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>

/**
 * @namespace impala
 */
namespace impala
{
/**
 * @namespace cache
 */
namespace cache
{
/**
 * Represent workers pool
 * nWorkers - number of worker threads.
 * If 0 is specified (default) - all available machine cores will be used
 */
template <int nWorkers = 0>
class ThreadPool
{
private:

    boost::asio::io_service m_ioService;         /** < service is responsible to perform asynchronous operations on behalf of c++ program*/
    boost::thread_group     m_threadGroup;       /** < worker threads */

    boost::shared_ptr<boost::asio::io_service::work> m_workCtrl;   /** < communicates io_service in words of "work is started" and "work is finished"
    											                    *  This way we know that io_service will not exit if some work is in progress
    											                    */
public:

   /* ctor. Initialize service-bound controller.
    * Create thread pool according to calculated / specified number of workers
    */
   ThreadPool(){
	   m_workCtrl.reset( new boost::asio::io_service::work(m_ioService) );

      // Get number of workers basing on number of machine cores
      int workers = boost::thread::hardware_concurrency();
      if(nWorkers > 0)
         workers = nWorkers;

      // Create threads in the pool
      for (std::size_t i = 0; i < workers; ++i)
      {
    	  m_threadGroup.create_thread(boost::bind(&boost::asio::io_service::run, &m_ioService, _1));
      }
   }

   virtual ~ThreadPool()
   {
	   // Release threads from the io_service.
	   // m_ioService.stop();
	   // Stop everything here:
	   m_workCtrl.reset();

	   // Wait for threads to finish
	   m_threadGroup.join_all();
	   delete m_workCtrl;
   }

   /**
    * Assign the task for one of workers.
    * Task is a functor
    *
    * @param task - task that should be executed by the worker thread.
    * Should be the Functor.
    */
   template <typename TTask>
   void add_task(TTask task)
   {
      // c++11
	  // m_ioService.dispatch(std::move(task));
	  // post the task and go out
	  m_ioService.post(std::move(task));
      // before c++11
      // io_service_.dispatch(task);
   }
};
} /** cache */
} /** impala */


#endif /* CACHE_WORK_POOL_H_ */
