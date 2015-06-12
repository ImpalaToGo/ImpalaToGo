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

/**
 * get busy space on disk
 * @param path - path to get busy space for
 *
 * @return covered space for path specified
 */
boost::uintmax_t get_dir_busy_space(const std::string& path);

/** reverse the linked list */
template<typename _Node>
void reverse(_Node& head) {
	// if there's 0 or 1 element in the list, do nothing:
	if(!head || !head->next())
		return;

	_Node prev;
	_Node current = head;
	_Node next = head;

	while(current) {
		next = current->next();
		current->next(prev);
		prev = current;
		current = next;
	};
	head = prev;
}

#include <vector>
#include <sstream>
#include <iterator>
#include <cstring>
#include <memory.h>

/** Represents a program invocation details : the program name and variable length arguments */
class ProgramInvocationDetails{
private:
	char*  m_program;   /**< fast reference to a program name */
	char** m_args;      /**< fast reference to arguments */
    char** m_c_tokens;  /**< tokens extracted from the command line separated with spaces */
    int    m_tokensNum; /**< number of tokens extracted */

    bool   m_valid;     /**< flag, indicates the validity of the details */

public:
	ProgramInvocationDetails(const std::string& cmd) :
		m_program(NULL), m_args(NULL), m_c_tokens(NULL), m_tokensNum(0), m_valid(false){
		// parse the command to instantiate members
		if(cmd.empty())
			return;

		// tokenize:
		std::istringstream buf(cmd);
		std::istream_iterator<std::string> beg(buf), end;
		std::vector<std::string> tokens(beg, end);

		if(tokens.size() == 0)
			return;

		// allocate an array to hold the program and arguments tokens pointers:
		m_c_tokens = new char*[tokens.size()];
		// populate an array:
		for(auto token : tokens){
			m_c_tokens[m_tokensNum] = new char[token.length() + 1];
            memset(m_c_tokens[m_tokensNum], 0, token.length() + 1);
			std::memcpy(m_c_tokens[m_tokensNum], token.c_str(), token.length());
			m_tokensNum++;
		}
		// point program and params to corresponding tokens locations:
		m_program = m_c_tokens[0];
		m_args = m_c_tokens;

		m_valid = true;
	}

	~ProgramInvocationDetails() {
		// go over the tokens and destroy them:
		for(int idx = 0; idx < m_tokensNum; idx++){
			delete [] m_c_tokens[idx];
		}
		// delete the tokens pointers storage:
		delete [] m_c_tokens;

		m_c_tokens = NULL;
	}

	/** getter for arguments if any specified */
	char** args() { return m_args; }

	/** getter for all the command tokens if any specified */
	char** argv() { return m_c_tokens; }

	/** getter from program if any specified */
	char* program() { return m_program;  }

	/** getter to validate the invocation details */
	bool valid() { return m_valid; }
};
}
}

#endif /* URI_UTIL_HPP_ */
