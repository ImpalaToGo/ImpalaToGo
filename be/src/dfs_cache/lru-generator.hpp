/*
 * @file  lru-generator.hpp
 * @brief Generator-continuation class to publish LRU Lifespan Manager as enumerable
 *
 * @date   Nov 11, 2014
 * @author elenav
 */

#ifndef LRU_GENERATOR_HPP_
#define LRU_GENERATOR_HPP_

#include <boost/shared_ptr.hpp>

#include "util/generator-coroutine.h"
#include "dfs_cache/lru-cache.hpp"

namespace impala{

/** generator - continuation for Lifepsan Manager */
template<typename Source_, typename ItemType_, typename PredicateStart_, typename PredicateGetNext_, typename PredicateGetGuard_>
$generator(lru_gen)
{
	Source_* m_source;         /**< generated data source */
	ItemType_ m_currentItem;   /**< current item to yield */

	PredicateStart_    m_predicateStart;     /**< predicate to get the start hint */
	PredicateGetNext_  m_predicateNext;      /**< predicate to get next item */
	PredicateGetGuard_ m_predicateGetGuard;  /**< predicate to get finalization guard hint */

	long long m_idx;   /**< current index */

	/** Ctor, provides everything needed to construct the generator
	 *
	 * @param source      - data source
	 * @param currentItem - current item
	 * @param start       - predicate to get the start hint
	 * @param next        - predicate to get next item
	 * @param guard       - predicate to get finalization guard hint
	 * */
	lru_gen(Source_* source, ItemType_ currentItem, PredicateStart_ start, PredicateGetNext_ next, PredicateGetGuard_ guard) :
	m_source(source), m_currentItem(currentItem), m_predicateStart(start), m_predicateNext(next), m_predicateGetGuard(guard){

		// default-initialize the current item to "undefined"~"finalization guard" value
		m_currentItem = m_predicateGetGuard();

        // initialize index using hint received from start predicate
        m_idx = m_predicateStart();
	}

	$emit(ItemType_) // declare that we are going to emit items of type ItemType_

	{
		// first go with default current item
		m_currentItem = m_predicateNext(m_idx, m_currentItem);
		if(m_currentItem != m_predicateGetGuard())
			$yield(m_currentItem);
	} while(m_currentItem != m_predicateGetGuard()); // and then until we have valid items produced

	$stop; // stop. End of coroutine body.
};

/** External source item loader with yield and continuation support */
template<typename KeyType_, typename ItemType_, typename PredicateConstructObject_, typename PredicateContinuation_>
$generator(item_loader)
{
	KeyType_             m_key;
	ItemType_*           m_item;

	PredicateConstructObject_ m_constructor;
	PredicateContinuation_    m_continuation;

	item_loader(KeyType_ key, PredicateConstructObject_ constructor, PredicateContinuation_ continuation) :
    	m_key(key), m_item(nullptr), m_constructor(constructor), m_continuation(continuation){}

	$emit(ItemType_*) // emit item of type ItemType_*

	do{
		m_item = m_constructor(m_key);

		// yield it to be caught from outside and preserve it locally:
		$yield(m_item);

		if(m_item != nullptr){
			// run continuation only for alive targets:
			m_continuation(m_item);
		}
		break;
	} while(true);

	$stop; // stop. End of coroutine body.
};
}

#endif /* LRU_GENERATOR_HPP_ */
