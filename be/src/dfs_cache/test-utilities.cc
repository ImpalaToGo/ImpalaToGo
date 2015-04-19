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
	const std::string TEST_DATASET_DEFAULT_LOCATION = "/root/ImpalaToGo/testdata/dfs_cache/";

	/** IMPALA_HOME environment variable name */
	const std::string IMPALA_HOME_ENV_VARIABLE_NAME = "IMPALA_HOME";

	/** Test cache location */
	const std::string TEST_CACHE_DEFAULT_LOCATION = "/cache/impalatogo/";

	/** reduced age bucket timeslice */
	const int TEST_CACHE_REDUCED_TIMESLICE = 10;

	/** dataset single file for tests operating with a single file */
	const std::string TEST_SINGLE_FILE_FROM_DATASET = "output1.dat";

	/** protocol prefix represents local fs */
	const std::string TEST_LOCALFS_PROTO_PREFFIX = "file:/";

	/** protocol prefix representing Tachyon fs */
	const std::string TEST_TACHYONFS_PROTO_PREFIX = "tachyon://";
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


