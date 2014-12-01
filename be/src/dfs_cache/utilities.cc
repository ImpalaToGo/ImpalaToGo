/*
 * @file  utilities.cc
 * @brief define some misc utilities used by cache layer
 *
 * @date   Nov 22, 2014
 * @author elenav
 */

#include <sstream>
#include <boost/filesystem.hpp>
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

time_t posix_time_to_time_t(boost::posix_time::ptime time)
{
    using namespace boost::posix_time;
    ptime epoch(boost::gregorian::date(1970, 1, 1));
    time_duration::sec_type time_duartion_in_sec = (time - epoch).total_seconds();
    return time_t(time_duartion_in_sec);
}

boost::uintmax_t get_free_space_on_disk(const std::string& path){
	boost::system::error_code ec;
	boost::filesystem::space_info  space_info = boost::filesystem::space(path,  ec);
	if(!ec)
		return space_info.available;
	return 0;
}

}
}


