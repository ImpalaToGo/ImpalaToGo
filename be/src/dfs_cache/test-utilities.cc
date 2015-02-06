/*
 * @file test-utilities.cc
 * @brief testing-purposes implementations
 *
 * @author elenav
 * @date   Oct 29, 2014
 */

#include "dfs_cache/test-utilities.hpp"

namespace impala{

namespace constants{
    /** Fixed cache size for tests require this setting */
	const int TEST_CACHE_FIXED_SIZE = 1048576;

	/** default percent of free space on the configured cache location to be considred by cache layer */
	const int TEST_CACHE_DEFAULT_FREE_SPACE_PERCENT = 95;

	/** Test dataset location */
	const std::string TEST_DATASET_DEFAULT_LOCATION = "/home/elenav/src/ImpalaToGo/testdata/dfs_cache/";

	/** Test cache location */
	const std::string TEST_CACHE_DEFAULT_LOCATION = "/var/cache/ImpalaToGo/";

	/** reduced age bucket timeslice */
	const int TEST_CACHE_REDUCED_TIMESLICE = 10;
}

int stringLength = sizeof(alphanum) - 1;

char genRandomChar(){
    return alphanum[rand() % stringLength];
}

std::string genRandomString(size_t len){
	srand(time(0));
	std::string str;
	for(unsigned int i = 0; i < len; ++i){
		str += genRandomChar();
	}
	return str;
}

}


