/*
 * sync-with-utilities.hpp
 *
 *  Created on: Nov 7, 2014
 *      Author: elenav
 */

#ifndef SYNC_WITH_UTILITIES_HPP_
#define SYNC_WITH_UTILITIES_HPP_


#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>

typedef boost::shared_mutex Lock;

/** Write lock, exclusive access */
typedef boost::unique_lock<Lock>  WriteLock;

/** Read lock, shared access */
typedef boost::shared_lock<Lock>  ReadLock;



#endif /* SYNC_WITH_UTILITIES_HPP_ */
