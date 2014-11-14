/*
 * @file uri-util.hpp
 * @brief define utility to parse uri as we face them in Impala
 *
 * @date   Oct 3, 2014
 * @author elenav
 */

#ifndef URI_UTIL_HPP_
#define URI_UTIL_HPP_

#include <string>
#include <algorithm>    // find

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
			result.Hierarchy = uri.substr(distance,
					fileNameStart - distance);
		}

		return result;

	}   // Parse
};
// uri

#endif /* URI_UTIL_HPP_ */
