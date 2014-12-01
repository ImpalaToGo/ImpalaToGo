/*
 * @file test-utilities.cc
 * @brief testing-purposes implementations
 *
 * @author elenav
 * @date   Oct 29, 2014
 */

#include "dfs_cache/test-utilities.hpp"

namespace impala{

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


