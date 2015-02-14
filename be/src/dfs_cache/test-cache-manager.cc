/**
 * @file test-cache-manager.ccr.cc
 * @brief contains tests for cache manager.
 * Content:
 * - EstmateDatasetTaskAsync       - request "Estimate dataset" task, for async execution, wait for it to execute (client emulation)
 * - EstmateDatasetTaskSync        - request "Estimate dataset" task, for sync execution
 * - EstmateDatasetFewTasksAsync   - using 6 different client contexts (assuming 6 clients are connected), for all of them request
 *                                   "Estimate dataset" task, for async execution. 2 scenarios:
 *                                   1) ask for requests via std::async so that runtime decides to spawn threads or run some in the current thread - on its own.
 *                                   2) ask for requests via explicit spawning a thread for each client.
 *
 *
 * - EstimateDatasetHeavyLoadManagedAsync  - the same as EstmateDatasetFewTasksAsync, both scenario, but with 700 parallel requests.
 *                                           Run requests in 700 parallel-spawned threads.
 * - EstimateDataseHeavyLoadUnmanagedAsync - the same as EstmateDatasetFewTasksAsync, both scenario, but with 700 parallel requests.
 * 											 Run requests via 700 std::async constructions.
 *
 * @author elenav
 * @date   Oct 29, 2014
 */

#include <string>
#include <gtest/gtest.h>
#include <boost/thread.hpp>

#include <stdlib.h>
#include <fcntl.h>
#include <stdio.h>

#include <algorithm>

#include "dfs_cache/filesystem-mgr.hpp"
#include "dfs_cache/cache-mgr.hpp"
#include "gtest-fixtures.hpp"
#include "dfs_cache/test-utilities.hpp"

namespace ph = std::placeholders;

namespace impala {

FileSystemDescriptor CacheLayerTest::m_dfsIdentityDefault;
FileSystemDescriptor CacheLayerTest::m_dfsIdentitylocalFilesystem;

SessionContext CacheLayerTest::m_ctx1 = nullptr;
SessionContext CacheLayerTest::m_ctx2 = nullptr;
SessionContext CacheLayerTest::m_ctx3 = nullptr;
SessionContext CacheLayerTest::m_ctx4 = nullptr;
SessionContext CacheLayerTest::m_ctx5 = nullptr;
SessionContext CacheLayerTest::m_ctx6 = nullptr;

TEST_F(CacheLayerTest, DISABLED_AddEstmateDatasetTaskAsync){
    m_flag = false;
	// Emulate user connected:
	std::string* timur = new std::string("Timur");
	m_ctx1 = static_cast<void*>(timur);
	// Schedule the dataset:
	DataSet data;
	data.push_back("/home/elenav/src/ImpalaToGo/datastorage/filename1.txt");
	data.push_back("/home/elenav/src/ImpalaToGo/datastorage/filename2.txt");
	data.push_back("/home/elenav/src/ImpalaToGo/datastorage/filename3.txt");

	CacheEstimationCompletedCallback cb = [&] (SessionContext context,
			const std::list<boost::shared_ptr<FileProgress> > & estimation,
			time_t const & time, bool overall, bool canceled, taskOverallStatus status) -> void {
		std::lock_guard<std::mutex> lock(m_mux);
        EXPECT_TRUE(status == taskOverallStatus::COMPLETED_OK);
		EXPECT_TRUE(context != NULL);

		// check that context is the one we expect for!
		std::string *sp = static_cast<std::string*>(context);
		// You could use 'sp' directly, or this, which does a copy.
		std::string s = *sp;

		EXPECT_EQ(s, *timur);
		EXPECT_EQ(sp, timur);
		EXPECT_TRUE(estimation.size() != 0);
		EXPECT_FALSE(canceled);
		EXPECT_TRUE(overall);

		m_flag = true;
        m_condition.notify_all();
	};

	time_t time = 0;
	requestIdentity identity;
	// execute request in async way:
    CacheManager::instance()->cacheEstimate(m_ctx1, m_dfsIdentityDefault, data, time, cb, identity);
    // wait when callback will be fired:

    std::unique_lock<std::mutex> lock(m_mux);
    m_condition.wait(lock, [this]{return m_flag;});
    delete static_cast<std::string*>(m_ctx1);
}

TEST_F(CacheLayerTest, DISABLED_AddEstmateDatasetTaskSync){

	m_flag = false;
	// Emulate user connected:
	std::string* timur = new std::string("Timur");
	m_ctx1 = static_cast<void*>(timur);
	// Schedule the dataset:
	DataSet data;
	data.push_back("/home/elenav/src/ImpalaToGo/datastorage/filename1.txt");
	data.push_back("/home/elenav/src/ImpalaToGo/datastorage/filename2.txt");
	data.push_back("/home/elenav/src/ImpalaToGo/datastorage/filename3.txt");

	CacheEstimationCompletedCallback cb = [&] (SessionContext context,
			const std::list<boost::shared_ptr<FileProgress> > & estimation,
			time_t const & time, bool overall, bool canceled, taskOverallStatus status) -> void {
		std::lock_guard<std::mutex> lock(m_mux);
        EXPECT_TRUE(status == taskOverallStatus::COMPLETED_OK);
		EXPECT_TRUE(context != NULL);

		// check that context is the one we expect for!
		std::string *sp = static_cast<std::string*>(context);
		// You could use 'sp' directly, or this, which does a copy.
		std::string s = *sp;

		EXPECT_EQ(s, *timur);
		EXPECT_EQ(sp, timur);

		EXPECT_TRUE(estimation.size() != 0);
		EXPECT_FALSE(canceled);
		EXPECT_TRUE(overall);

		m_flag = true;
        m_condition.notify_all();
	};

	time_t time = 0;
	requestIdentity identity;
	// execute request in a sync way:
    CacheManager::instance()->cacheEstimate(m_ctx1, m_dfsIdentityDefault, data, time, cb, identity, false);
    // wait when callback will be fired:

    std::unique_lock<std::mutex> lock(m_mux);
    m_condition.wait(lock, [this]{return m_flag;});

    delete static_cast<std::string*>(m_ctx1);
}

TEST_F(CacheLayerTest, DISABLED_AddFewEstmateDatasetTaskAsync){

	m_flag = false;
	// Emulate user connected:
		std::string* timur     = new std::string("Timur");
		std::string* me        = new std::string("me");
		std::string* miniscule = new std::string("miniscule");
		std::string* monster   = new std::string("monster");
		std::string* dragon    = new std::string("dragon");
		std::string* tweety    = new std::string("tweety");

		m_ctx1 = static_cast<void*>(timur);
		m_ctx2 = static_cast<void*>(me);
		m_ctx3 = static_cast<void*>(miniscule);
		m_ctx4 = static_cast<void*>(monster);
		m_ctx5 = static_cast<void*>(dragon);
		m_ctx6 = static_cast<void*>(tweety);

		int countdown = 6 * 2;

		// Schedule the dataset:
		DataSet data;
		data.push_back("/home/elenav/src/ImpalaToGo/datastorage/filename1.txt");
		data.push_back("/home/elenav/src/ImpalaToGo/datastorage/filename2.txt");
		data.push_back("/home/elenav/src/ImpalaToGo/datastorage/filename3.txt");

		CacheEstimationCompletedCallback cb = [&] (SessionContext context,
				const std::list<boost::shared_ptr<FileProgress> > & estimation,
				time_t const & time, bool overall, bool canceled, taskOverallStatus status) -> void {
			std::lock_guard<std::mutex> lock(m_mux);
	        EXPECT_TRUE(status == taskOverallStatus::COMPLETED_OK);
			EXPECT_TRUE(context != NULL);

			countdown--;

			EXPECT_TRUE(estimation.size() != 0);
			EXPECT_FALSE(canceled);
			EXPECT_TRUE(overall);

			if(countdown == 0){ // finalize the test when all requests back with a callback
				m_flag = true;
				m_condition.notify_all();
			}
		};

		time_t time_ = 0;
		requestIdentity identity;
		// execute all requests in async way:

		using namespace std::placeholders;

		auto f1 = std::bind(&CacheManager::cacheEstimate, CacheManager::instance(), ph::_1, ph::_2, ph::_3, ph::_4, ph::_5, ph::_6, ph::_7);

		auto fumanual1 = spawn_task(f1, m_ctx1, std::cref(m_dfsIdentityDefault), std::cref(data), std::ref(time_), cb, std::ref(identity), true);
		auto fumanual2 = spawn_task(f1, m_ctx2, std::cref(m_dfsIdentityDefault), std::cref(data), std::ref(time_), cb, std::ref(identity), true);
		auto fumanual3 = spawn_task(f1, m_ctx3, std::cref(m_dfsIdentityDefault), std::cref(data), std::ref(time_), cb, std::ref(identity), true);
		auto fumanual4 = spawn_task(f1, m_ctx4, std::cref(m_dfsIdentityDefault), std::cref(data), std::ref(time_), cb, std::ref(identity), true);
		auto fumanual5 = spawn_task(f1, m_ctx5, std::cref(m_dfsIdentityDefault), std::cref(data), std::ref(time_), cb, std::ref(identity), true);
		auto fumanual6 = spawn_task(f1, m_ctx6, std::cref(m_dfsIdentityDefault), std::cref(data), std::ref(time_), cb, std::ref(identity), true);

		status::StatusInternal status = fumanual1.get();
		EXPECT_EQ(status, status::StatusInternal::OPERATION_ASYNC_SCHEDULED);
		status = fumanual2.get();
		EXPECT_EQ(status, status::StatusInternal::OPERATION_ASYNC_SCHEDULED);
		status = fumanual3.get();
		EXPECT_EQ(status, status::StatusInternal::OPERATION_ASYNC_SCHEDULED);
		status = fumanual4.get();
		EXPECT_EQ(status, status::StatusInternal::OPERATION_ASYNC_SCHEDULED);
		status = fumanual5.get();
		EXPECT_EQ(status, status::StatusInternal::OPERATION_ASYNC_SCHEDULED);
		status = fumanual6.get();
		EXPECT_EQ(status, status::StatusInternal::OPERATION_ASYNC_SCHEDULED);

		auto future1 = std::async(std::launch::async, [&]{ return f1(m_ctx1, std::cref(m_dfsIdentityDefault), std::cref(data), std::ref(time_), cb, std::ref(identity), true); });
		auto future2 = std::async(std::launch::async, [&]{ return f1(m_ctx2, std::cref(m_dfsIdentityDefault), std::cref(data), std::ref(time_), cb, std::ref(identity), true); });
		auto future3 = std::async(std::launch::async, [&]{ return f1(m_ctx3, std::cref(m_dfsIdentityDefault), std::cref(data), std::ref(time_), cb, std::ref(identity), true); });
		auto future4 = std::async(std::launch::async, [&]{ return f1(m_ctx4, std::cref(m_dfsIdentityDefault), std::cref(data), std::ref(time_), cb, std::ref(identity), true); });
		auto future5 = std::async(std::launch::async, [&]{ return f1(m_ctx5, std::cref(m_dfsIdentityDefault), std::cref(data), std::ref(time_), cb, std::ref(identity), true); });
		auto future6 = std::async(std::launch::async, [&]{ return f1(m_ctx6, std::cref(m_dfsIdentityDefault), std::cref(data), std::ref(time_), cb, std::ref(identity), true); });

		status = future1.get();
		EXPECT_EQ(status, status::StatusInternal::OPERATION_ASYNC_SCHEDULED);
		status = future2.get();
		EXPECT_EQ(status, status::StatusInternal::OPERATION_ASYNC_SCHEDULED);
		status = future3.get();
		EXPECT_EQ(status, status::StatusInternal::OPERATION_ASYNC_SCHEDULED);
		status = future4.get();
		EXPECT_EQ(status, status::StatusInternal::OPERATION_ASYNC_SCHEDULED);
		status = future5.get();
		EXPECT_EQ(status, status::StatusInternal::OPERATION_ASYNC_SCHEDULED);
		status = future6.get();
		EXPECT_EQ(status, status::StatusInternal::OPERATION_ASYNC_SCHEDULED);

	    // wait when callback will be fired 6 times:
	    std::unique_lock<std::mutex> lock(m_mux);
	    m_condition.wait(lock, [this]{return m_flag;});

	    lock.unlock();

	    delete static_cast<std::string*>(m_ctx1);
	    delete static_cast<std::string*>(m_ctx2);
	    delete static_cast<std::string*>(m_ctx3);
	    delete static_cast<std::string*>(m_ctx4);
	    delete static_cast<std::string*>(m_ctx5);
	    delete static_cast<std::string*>(m_ctx6);
}

TEST_F(CacheLayerTest, DISABLED_EstimateDatasetHeavyLoadManagedAsync){
	    m_flag = false;

        const int CONTEXT_NUM = 700;
		std::atomic<int> countdown(CONTEXT_NUM);

		// Schedule the dataset:
		DataSet data;
		data.push_back("/home/elenav/src/ImpalaToGo/datastorage/filename1.txt");
		data.push_back("/home/elenav/src/ImpalaToGo/datastorage/filename2.txt");
		data.push_back("/home/elenav/src/ImpalaToGo/datastorage/filename3.txt");

		CacheEstimationCompletedCallback cb = [&] (SessionContext context,
				const std::list<boost::shared_ptr<FileProgress> > & estimation,
				time_t const & time, bool overall, bool canceled, taskOverallStatus status) -> void {
			std::lock_guard<std::mutex> lock(m_mux);
	        EXPECT_TRUE(status == taskOverallStatus::COMPLETED_OK);
			EXPECT_TRUE(context != NULL);

			EXPECT_TRUE(estimation.size() != 0);
			EXPECT_FALSE(canceled);
			EXPECT_TRUE(overall);

			if(--countdown == 0){ // finalize the test when all requests back with a callback
				m_flag = true;
				m_condition.notify_all();
			}
		};

		time_t time_ = 0;
		requestIdentity identity;
		// execute all requests in async way:

		using namespace std::placeholders;

		auto f1 = std::bind(&CacheManager::cacheEstimate, CacheManager::instance(), ph::_1, ph::_2, ph::_3, ph::_4, ph::_5, ph::_6, ph::_7);

		std::vector<SessionContext> clients;
		for(int i = 0; i < CONTEXT_NUM; i++){
			// generate unique client.
			std::string* client = new std::string(genRandomString(10));
			SessionContext ctx  = static_cast<void*>(client);
			clients.push_back(ctx);
		}

		status::StatusInternal status;

		std::vector<std::future<status::StatusInternal>> futures;
		for(int i = 0; i < CONTEXT_NUM; i++){
			srand(time(0));       //initialize the random seed
			int idx;
			if(CONTEXT_NUM == 1)
				idx = 0;
			else
				idx = rand() % (CONTEXT_NUM - 1); //generates a random number between 0 and 1000
			futures.push_back(std::move(spawn_task(f1, clients[idx], std::cref(m_dfsIdentityDefault), std::cref(data), std::ref(time_), cb, std::ref(identity), true)));
		}


		for(int i = 0; i < CONTEXT_NUM; i++){
			if(futures[i].valid())
				status = futures[i].get();
			if(status == status::StatusInternal::OPERATION_ASYNC_REJECTED){
				--countdown;
			}
		}

        EXPECT_EQ(futures.size(), CONTEXT_NUM);

	    // wait when callback will be fired 6 times:
	    std::unique_lock<std::mutex> lock(m_mux);
	    m_condition.wait(lock, [&]{ return m_flag || countdown == 0;});

		for(int i = 0; i < CONTEXT_NUM; i++){
			delete static_cast<std::string*>(clients[i]);
		}
		lock.unlock();
}

TEST_F(CacheLayerTest, DISABLED_EstimateDataseHeavyLoadUnmanagedAsync){
	    m_flag = false;

        const int CONTEXT_NUM = 700;
		std::atomic<int> countdown(CONTEXT_NUM);

		// Schedule the dataset:
		DataSet data;
		data.push_back("/home/elenav/src/ImpalaToGo/datastorage/filename1.txt");
		data.push_back("/home/elenav/src/ImpalaToGo/datastorage/filename2.txt");
		data.push_back("/home/elenav/src/ImpalaToGo/datastorage/filename3.txtt");

		CacheEstimationCompletedCallback cb = [&] (SessionContext context,
				const std::list<boost::shared_ptr<FileProgress> > & estimation,
				time_t const & time, bool overall, bool canceled, taskOverallStatus status) -> void {
			std::lock_guard<std::mutex> lock(m_mux);
	        EXPECT_TRUE(status == taskOverallStatus::COMPLETED_OK);
			EXPECT_TRUE(context != NULL);

			EXPECT_TRUE(estimation.size() == data.size());
			EXPECT_FALSE(canceled);
			EXPECT_TRUE(overall);

			if(--countdown == 0){ // finalize the test when all requests back with a callback
				m_flag = true;
				m_condition.notify_all();
			}
		};

		time_t time_ = 0;
		requestIdentity identity;
		// execute all requests in async way:

		using namespace std::placeholders;

		auto f1 = std::bind(&CacheManager::cacheEstimate, CacheManager::instance(), ph::_1, ph::_2, ph::_3, ph::_4, ph::_5, ph::_6, ph::_7);

		std::vector<SessionContext> clients;
		for(int i = 0; i < CONTEXT_NUM; i++){
			// generate unique client.
			std::string* client = new std::string(genRandomString(10));
			SessionContext ctx  = static_cast<void*>(client);
			clients.push_back(ctx);
		}

		status::StatusInternal status;

		std::vector<std::future<status::StatusInternal>> futures;
		// the same but rely on system:
		for(int i = 0; i < CONTEXT_NUM; i++){
			srand(time(0));       //initialize the random seed
			int idx;
			if(CONTEXT_NUM == 1)
				idx = 0;
			else
				idx = rand() % (CONTEXT_NUM - 1); //generates a random number between 0 and CONTEXT_NUM
			futures.push_back(std::async(std::launch::async, [&]{ return f1(clients[idx], std::cref(m_dfsIdentityDefault), std::cref(data), std::ref(time_), cb, std::ref(identity), true); }));
		}

		EXPECT_EQ(futures.size(), CONTEXT_NUM);

		for(int i = 0; i < CONTEXT_NUM; i++){
			status = futures[i].get();
			if(status == status::StatusInternal::OPERATION_ASYNC_REJECTED){
				--countdown;
			}
		}

		// wait when callback will be fired 6 times:
	    std::unique_lock<std::mutex> lock(m_mux);
	    m_condition.wait(lock, [&]{ return m_flag || countdown == 0;});

		for(int i = 0; i < CONTEXT_NUM; i++){
			delete static_cast<std::string*>(clients[i]);
		}
		lock.unlock();
}

TEST_F(CacheLayerTest, DISABLED_PrepareDatasetHeavyLoadManagedAsync){
	    m_flag = false;

        const int CONTEXT_NUM = 1;
		std::atomic<int> countdown(CONTEXT_NUM);

		// Schedule the dataset:
		DataSet data;
		data.push_back("/home/elenav/src/ImpalaToGo/datastorage/filename1.txt");
		data.push_back("/home/elenav/src/ImpalaToGo/datastorage/filename2.txt");
		data.push_back("/home/elenav/src/ImpalaToGo/datastorage/filename3.txt");

		PrepareCompletedCallback cb = [&] (SessionContext context,
				const std::list<boost::shared_ptr<FileProgress> > & progress,
				request_performance const & performance, bool overall,
								bool canceled, taskOverallStatus status) -> void {
			std::lock_guard<std::mutex> lock(m_mux);
	        EXPECT_TRUE(status == taskOverallStatus::COMPLETED_OK);
			EXPECT_TRUE(context != NULL);

			EXPECT_TRUE(progress.size() == data.size());
			EXPECT_FALSE(canceled);
			EXPECT_TRUE(overall);

			if(--countdown == 0){ // finalize the test when all requests back with a callback
				m_flag = true;
				m_condition.notify_all();
			}
		};

		time_t time_ = 0;
		requestIdentity identity;
		// execute all requests in async way:

		using namespace std::placeholders;

		auto f1 = std::bind(&CacheManager::cachePrepareData, CacheManager::instance(), ph::_1, ph::_2, ph::_3, ph::_4, ph::_5);

		std::vector<SessionContext> clients;
		for(int i = 0; i < CONTEXT_NUM; i++){
			// generate unique client.
			std::string* client = new std::string(genRandomString(10));
			SessionContext ctx  = static_cast<void*>(client);
			clients.push_back(ctx);
		}

		status::StatusInternal status;

		std::vector<std::future<status::StatusInternal>> futures;
		for(int i = 0; i < CONTEXT_NUM; i++){
			srand(time(0));       //initialize the random seed
			int idx;
			if(CONTEXT_NUM == 1)
				idx = 0;
			else
				idx = rand() % (CONTEXT_NUM - 1); //generates a random number between 0 and CONTEXT_NUM
			futures.push_back(std::move(spawn_task(f1, clients[idx], std::cref(m_dfsIdentityDefault), std::cref(data), cb, std::ref(identity))));
		}


		for(int i = 0; i < CONTEXT_NUM; i++){
			if(futures[i].valid())
				status = futures[i].get();
			if(status == status::StatusInternal::OPERATION_ASYNC_REJECTED){
				--countdown;
			}
		}

        EXPECT_EQ(futures.size(), CONTEXT_NUM);

	    // wait when callback will be fired 6 times:
	    std::unique_lock<std::mutex> lock(m_mux);
	    m_condition.wait(lock, [&]{ return m_flag || countdown == 0;});

		for(int i = 0; i < CONTEXT_NUM; i++){
			delete static_cast<std::string*>(clients[i]);
		}
		lock.unlock();
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();

}


