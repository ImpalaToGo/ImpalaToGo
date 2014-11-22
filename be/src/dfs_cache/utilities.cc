/*
 * @file  utilities.cc
 * @brief define some misc utilities used by cache layer
 *
 * @date   Nov 22, 2014
 * @author elenav
 */

#include <sstream>
#include "dfs_cache/utilities.hpp"

namespace impala
{
namespace utilities{

std::vector<std::string> &split(const std::string &original, char delimiter, std::vector<std::string> &elements){
    std::stringstream ss(original);
    std::string item;
    while (std::getline(ss, item, delimiter)) {
    	elements.push_back(item);
    }
    return elements;
}

std::vector<std::string> split(const std::string &original, char delimiter) {
    std::vector<std::string> elems;
    split(original, delimiter, elems);
    return elems;
}

bool endsWith (const std::string& original, const std::string& ending)
{
    if (original.length() >= ending.length()) {
        return (0 == original.compare (original.length() - ending.length(), ending.length(), ending));
    } else {
        return false;
    }
}
}
}


