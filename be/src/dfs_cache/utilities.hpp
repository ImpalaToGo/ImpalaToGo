/*
 * @file utilities.hpp
 * @brief define some misc utilities used by cache layer
 *
 * @date   Oct 3, 2014
 * @author elenav
 */

#ifndef URI_UTIL_HPP_
#define URI_UTIL_HPP_

#include <string>
#include <algorithm>    // find

#include <boost/algorithm/string.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/posix_time/time_formatters.hpp>

struct Uri {
public:
	std::string QueryString, Path, Protocol, Host, Port, FilePath, Hierarchy;

	static Uri Parse(const std::string &uri) {
		Uri result;

		typedef std::string::const_iterator iterator_t;

		if (uri.length() == 0)
			return result;

		iterator_t uriEnd = uri.end();

		// get query start
		iterator_t queryStart = std::find(uri.begin(), uriEnd, '?');

		size_t fileNameStart = uri.find_last_of("/\\");

		// protocol
		iterator_t protocolStart = uri.begin();
		iterator_t protocolEnd = std::find(protocolStart, uriEnd, ':'); //"://");

		if (protocolEnd != uriEnd) {
			std::string prot = &*(protocolEnd);
			if ((prot.length() > 3) && (prot.substr(0, 3) == "://")) {
				result.Protocol = std::string(protocolStart, protocolEnd);
				protocolEnd += 3;   //      ://
			} else
				protocolEnd = uri.begin();  // no protocol
		} else
			protocolEnd = uri.begin();  // no protocol

		// host
		iterator_t hostStart = protocolEnd;
		iterator_t pathStart = std::find(hostStart, uriEnd, L'/'); // get pathStart

		iterator_t hostEnd = std::find(protocolEnd,
				(pathStart != uriEnd) ? pathStart : queryStart, L':'); // check for port

		result.Host = std::string(hostStart, hostEnd);

		// port
		if ((hostEnd != uriEnd) && ((&*(hostEnd))[0] == L':')) // we have a port
				{
			hostEnd++;
			iterator_t portEnd = (pathStart != uriEnd) ? pathStart : queryStart;
			result.Port = std::string(hostEnd, portEnd);
		}

		// path
		if (pathStart != uriEnd)
			result.Path = std::string(pathStart, queryStart);

		// filepath
		if (pathStart != uriEnd)
			result.FilePath = std::string(pathStart, uri.end());

		// query
		if (queryStart != uriEnd)
			result.QueryString = std::string(queryStart, uri.end());

		// hierarchy
		if (pathStart != uriEnd) {
			auto distance = std::distance(uri.begin(), pathStart);
			result.Hierarchy = uri.substr(distance, fileNameStart - distance);
		}

		return result;

	}   // Parse
};
// uri

namespace impala {

/** Giving the boost::shared_ptr<T> to nothing (nullptr) */
class {
public:
	template<typename T>
	operator boost::shared_ptr<T>() {
		return boost::shared_ptr<T>();
	}
} nullPtr;

namespace utilities {

/** split the given string basing on given delimiter
 *
 * @param [in]     original  - original string
 * @param [in]     delimiter - char delimiter
 * @param [in/out] elements  - original string parts split by delimiter
 *
 * @return split elements vector
 */
std::vector<std::string> &split(const std::string &original, char delimiter,
		std::vector<std::string> &elements);

/** split the given string basing on given delimiter
 * @param [in]     original  - original string
 * @param [in]     delimiter - char delimiter
 *
 * @return split elements vector
 */
std::vector<std::string> split(const std::string &original, char delimiter);

/**
 * check whether the @a original string ends with @a trailing string
 *
 * @param original - original string
 * @param ending   - string that need to be checked the original is ending with
 *
 * @return true if @a original ends with @a ending
 */
bool endsWith(const std::string& fullString, const std::string& ending);

/**
 * converts posix time to time_t
 * @param time - boost posix time time
 */
time_t posix_time_to_time_t(boost::posix_time::ptime time);

/** Case-insensitive comparator for strings */
struct insensitive_compare: public std::unary_function<std::string, bool> {
	explicit insensitive_compare(const std::string &baseline) :
			baseline(baseline) {
	}

	bool operator()(const std::string &arg) {
		return boost::iequals(arg, baseline);
	}
	std::string baseline;
};

/**
 * get free space for specified path
 * @param path - path to get the available space for
 *
 * @return available space for path specified
 */
boost::uintmax_t get_free_space_on_disk(const std::string& path);

/** reverse the linked list */
template<typename _Node>
void reverse(_Node& head) {
	_Node pred;
	_Node one = head;
	_Node two = head;
	do {
		two = two->next();
		one->next(pred);
		pred = one;
		one = two;
	}while(two->next());
	two->next(pred);
	head = two;
}

}
}

#endif /* URI_UTIL_HPP_ */
