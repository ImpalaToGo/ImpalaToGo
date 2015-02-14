/*
 * @file test-utilities.hpp
 * @brief utilities for testing purposes
 *
 * @author elenav
 * @date   Oct 29, 2014
 */

#ifndef TEST_UTILITIES_HPP_
#define TEST_UTILITIES_HPP_


#include <cstddef>
#include <utility>
#include <future>
#include <type_traits>

#include <iostream>
#include <string>
#include <cstdlib>
#include <ctime>
#include <vector>

namespace impala{
namespace constants{
    /** Fixed cache size for tests require this setting */
	extern const int TEST_CACHE_FIXED_SIZE;

	/** default percent of free space on the configured cache location to be considred by cache layer */
	extern const int TEST_CACHE_DEFAULT_FREE_SPACE_PERCENT;

	/** Test dataset location */
	extern const std::string TEST_DATASET_DEFAULT_LOCATION;

	/** IMPALA_HOME environment variable name */
	extern const std::string IMPALA_HOME_ENV_VARIABLE_NAME;

	/** Test cache location */
	extern const std::string TEST_CACHE_DEFAULT_LOCATION;

	/** reduced age bucket timeslice */
	extern const int TEST_CACHE_REDUCED_TIMESLICE;

	/** dataset single file for tests operating with a single file */
	extern const std::string TEST_SINGLE_FILE_FROM_DATASET;

	/** protocol prefix represents local fs */
	extern const std::string TEST_LOCALFS_PROTO_PREFFIX;
}


template<typename Function_, typename A>
std::future<typename std::result_of<Function_(A&&)>::type>
st(Function_ f, A&& a){
	typedef typename std::result_of<Function_(A&&)>::type result_type;
	std::packaged_task<result_type(A&&)> task(std::move(f));
	std::future<result_type> res(task.get_future());

	std::thread t(std::move(task), std::move(a));
	t.detach();
	return res;
}

template <typename T_>
using decay_t = typename std::decay<T_>::type;

template< class T >
using result_of_t = typename std::result_of<T>::type;

template<typename Function_, typename ...Args_>
std::future<typename std::result_of<Function_(Args_...)>::type >
spawn_task(Function_&& function, Args_... args){

	typedef typename std::result_of<Function_(Args_...)>::type result_type;
	std::packaged_task<result_type(Args_...)> task(std::forward<Function_>(function));

	std::future<result_type> result(task.get_future());

	std::thread t(std::move(task), std::forward<Args_>(args)...);
	t.detach();

	return result;
}

template<typename Function_, typename ...Args_>
std::future<typename std::result_of<Function_(Args_...)>::type >
spawn_task1(Function_&& function, Args_&&... args){

	typedef typename std::result_of<Function_(Args_...)>::type result_type;
	//std::packaged_task<result_type(Args_...)> task(std::forward<Function_>(function));
	std::packaged_task<result_type(Args_...)> task(std::forward<Function_>(function));

	std::future<result_type> result(task.get_future());

	std::thread t(std::move(task), std::forward<Args_>(args)...);
	//t.detach();

	return result;
}

/** characters we will use to generate random string*/
static const char alphanum[] =
"0123456789"
"!@#$%^&*"
"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
"abcdefghijklmnopqrstuvwxyz";

extern int stringLength;

extern char genRandomChar();
extern std::string genRandomString(size_t len);

template<typename Item>
Item getRandomFromVector(std::vector<Item>& dataset ){
	size_t len = dataset.size() - 1;
    srand(time(0));
    return dataset[rand() % len];
}
}


#endif /* TEST_UTILITIES_HPP_ */
