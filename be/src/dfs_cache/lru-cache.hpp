/*
 * @file lru-cache.hpp
 * @brief LRU cache is created OTO local file system folder.
 * It is built using following architecture:
 * - indexes, fast unordered maps (hash maps), thus, items are available via indexes for read / write
 * - Lifespan Manager collection of Age Buckets, unordered map, where the key of a Bucket is the item last usage timestamp in hours representation.
 * Lifespan Manager divides items to these Buckets according to "time slice" - parameter that specify the Bucket span.
 * By default, the Bucket span is 6 hours which is
 *
 * @date Nov 7, 2014
 * @time elenav
 */

#ifndef LRU_CACHE_HPP_
#define LRU_CACHE_HPP_

#include <boost/weak_ptr.hpp>
#include <boost/function.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/posix_time/time_formatters.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/make_shared.hpp>

#include <boost/bind.hpp>

#include <atomic>
#include <unordered_map>
#include <algorithm>
#include <string>
#include <stdexcept>

#include "dfs_cache/sync-with-utilities.hpp"
#include "dfs_cache/lru-generator.hpp"
#include "dfs_cache/utilities.hpp"
#include "common/logging.h"

namespace impala{

/** Represents LRU (least recent used ) cache */
template<typename ItemType_>
class LRUCache {
public:
	/**********************  Predicates **********************************/

    /** "get key by item" predicate */
	template<typename KeyType_>
	using GetKeyFunc = typename boost::function<KeyType_(ItemType_* item)>;

    /** "get item by key" predicate */
	template<typename KeyType_>
	using LoadItemFunc = typename boost::function<void(ItemType_* item)>;

	/** predicate to construct/acquire externally the cache-managed object using the key and the "weight is changed" event*/
	template<typename KeyType_>
	using ConstructItemFunc = typename boost::function<ItemType_*(KeyType_ key)>;

	/** "get capacity limit" predicate */
	using TellCapacityLimitPredicate = typename boost::function<size_t()>;

	/** "get item weight" predicate */
	using TellWeightPredicate = typename boost::function<long long(ItemType_* item)>;

	/** "check item can be removed" predicate.
	 * @param item - item to check for removal approval
	 *
	 * @return predicate to try mark the item for deletion*/
	using MarkItemForDeletion = typename boost::function<bool(ItemType_* item)>;

	/** "get the item timestamp" predicate */
	using TellItemTimestamp = typename boost::function<boost::posix_time::ptime(ItemType_* item)>;

	/** Setter provided by external management to update the item timestamp from cache */
	using AcceptAssignedTimestamp = typename boost::function<void(ItemType_* item, const boost::posix_time::ptime& time)>;

	/** "item deletion" external call predicate, its up to implementor what to do with the item when it is deleted from the cache */
	using ItemDeletionPredicate = typename boost::function<bool(ItemType_* item, bool physically)>;

    /** to validate weak references to items */
    typedef boost::function<bool()> isValidPredicate;

    /** iterator for keys list */
    typedef std::list<long long>::const_iterator keys_iterator;

    /** Represents Lifespan Manager.Node */
     class INode {
     public:
     	ItemType_* m_item;   /**< underlying item to store */

     	/** set node underlying value */
     	void value(ItemType_* item) { m_item = item; };
     	virtual ~INode() = default;

     	/** get node underlying value */
     	ItemType_*  value()  { return m_item; }

     	virtual bool touch (bool first = false)        = 0;  /** method that marks the item as "accessed", "first" flag is to know whether
     														     the node is new */
        virtual bool remove(bool cleanup = true)       = 0;  /** remove the node. Default usage scenario is cleanup (with physical removal) */
        virtual size_t weight() 					   = 0;  /** tell weight of underlying item*/

     };

protected:
    isValidPredicate           m_isValid;                     /**< predicate to be invoked to check for vaidity */
    TellCapacityLimitPredicate m_tellCapacityLimitPredicate;  /**< predicate to be invoked to get the limit of capacity. For capacity planning */
    TellWeightPredicate        m_tellWeightPredicate;         /**< predicate to be invoked to get the weight of item. For cleanup planning */
    MarkItemForDeletion        m_markForDeletion;             /**< predicate to mark the item for deletion. */
    TellItemTimestamp          m_tellItemTimestamp;           /**< predicate to tell item timestamp */
    AcceptAssignedTimestamp    m_acceptAssignedTimestamp;     /**< predicate to update external item with assigned timestamp */
    ItemDeletionPredicate      m_itemDeletionPredicate;       /**< predicate to run externally when the item is removed from the cache */

    mutable std::atomic<long long>  m_currentCapacity;    /**< current cache capacity, in regards to capacity units configured.
                                                               represents  real weight of whole cache data */
    long long                       m_capacityLimit;      /**< cache capacity limit, configurable. We use 90% from configured value */
private:

    /** Internal Index API between Cache Manager and Indexes */
    class IIndexInternal{
    public:

    	virtual ~IIndexInternal() = default;

    	/** clear index */
        virtual void  clearIndex() = 0;

        /** add the node under this index */
        virtual bool  add(boost::shared_ptr<INode> item) = 0;

        /** lookup the item within the index */
        virtual boost::shared_ptr<INode> findItem(ItemType_* item) = 0;

        /** refresh index */
        virtual int rebuildIndex() = 0;
    };

public:

    /** Represents Managed Index public API */
    template<typename KeyType_>
    class IIndex {
    	public:
    	virtual ~IIndex() = default;

    	/**
    	 * index getter, look for item under the specified key within the index
    	 *
    	 * @param key     - key to find
    	 *
    	 * @return the value of object associated with the cache
    	 */
    	virtual ItemType_* operator [](KeyType_ key) = 0;

    	/**
    	 * Delete object that matches key from cache
    	 *
    	 * @param key  - key to remove from cache
    	 * @physically - flag, indicates whether physical removal is required
    	 */
    	virtual bool remove(KeyType_ key, bool physically = true) = 0;
    };

private:
    /*********************************** Internals *************************************************/

    /** Index provides map key / value access to any object in cache */
	template<typename KeyType_>
	class Index : public IIndex<KeyType_>, public IIndexInternal {
		private:
		LRUCache<ItemType_>* m_owner;  /**< associated Cache */

        std::unordered_map<KeyType_, boost::weak_ptr<INode> > m_index; 	/**< index set */

        Lock m_rwLock;  /**< read-write lock*/

        /** index iterator */
        typedef typename std::unordered_map<KeyType_, boost::weak_ptr<INode> >::iterator indexIterator;

        /** predicate for getting the key dedicated for specified value */
        GetKeyFunc<KeyType_>   m_getKey;

        /** predicate load the item into the cache is one is requested but is not here yet */
        LoadItemFunc<KeyType_> m_loadItem;

        /** predicate to construct new item to host it within the cache */
        ConstructItemFunc<KeyType_> m_constructItemPredicate;

        /** get the node by key */
        boost::shared_ptr<INode> getNode(KeyType_ key){
        	ReadLock lock(m_rwLock);
        	auto it = m_index.find(key);
        	// no node found under the key specified:
        	if(it == m_index.end())
        		return nullPtr;

        	// just check that the node is still alive:
        	boost::shared_ptr<INode> node = it->second.lock();
        	if(node){
        		return node;
        	}
        	return nullPtr;
        }

		public:

        ~Index() = default;

        /**
         * Construct an index
         *
         * @param owner    		- bound LRU cache
         * @param getKey   		- "get key" predicate
         * @param loadItem 		- "load item" predicate (if it was not found within an index)
         * @param constructItem - "construct item" predicate (basing on key specified)
         */
        Index(LRUCache<ItemType_>* owner, GetKeyFunc<KeyType_>& getKey, LoadItemFunc<KeyType_>& loadItem,
        		ConstructItemFunc<KeyType_>& constructItem)
        {
        	if(owner == nullptr){
        		return;
        	}
        	if(getKey == nullptr){
        		return;
        	}
        	m_owner = owner;
            m_getKey = getKey;
            m_loadItem = loadItem;
            m_constructItemPredicate = constructItem;

            rebuildIndex();
        }

        /** Index getter
         * @param key - key to find
         *
         * @return node underlying value-item. If no node exists, rise and invalid_argument exception
         */
        const ItemType_* operator [](KeyType_ key) const{
        	INode* node = getNode(key);

        	if(!node){
        		// if autoload is configured, invoke it to get the item into the cache
        		if(!m_loadItem || !m_constructItemPredicate)
        			return nullptr;

        		ItemType_* item;
        		bool success = false;
        		bool duplicate = false;

        		item_loader<KeyType_, ItemType_, ConstructItemFunc<KeyType_>, LoadItemFunc<KeyType_> >
        			loader(key, m_constructItemPredicate, m_loadItem);
        		while(loader(item)){
        			// if object construction was non-successful, reply nullptr
        			if(item == nullptr)
        				return nullptr;
        			node = m_owner->addInternal(item, success, duplicate);
        		}
        		// here,
        		if(!node)
        			return nullptr;
        	}
        	// check node value. It may be lazy-erased
        	if(node->value() == nullptr)
        		return nullptr;

        	node->touch();
        	return node->value();
        }

        /** Index getter
         *  @param key      - key to find
         *
         *  @return node underlying value-item. If no node exists, rise and invalid_argument exception
         */
        ItemType_* operator [](KeyType_ key){
        	boost::shared_ptr<INode> node = getNode(key);

        	if(!node){
        		// if autoload is configured, invoke it to get the item into the cache
        		if(!m_loadItem || !m_constructItemPredicate)
        			return nullptr;

        		ItemType_* item;
        		bool success   = false;
        		bool duplicate = false;

        		item_loader<KeyType_, ItemType_, ConstructItemFunc<KeyType_>, LoadItemFunc<KeyType_> >
        			loader(key, m_constructItemPredicate, m_loadItem);
        		while(loader(item)){
        			// if object construction was non-successful, reply nullptr
        			if(item == nullptr)
        				return nullptr;
        			node = m_owner->addInternal(item, success, duplicate);
        		}
        		if(!node)
        			return nullptr;
        	}
        	// check node value. It may be lazy-erased
        	if(node->value() == nullptr)
        		return nullptr;

        	node->touch();
        	return node->value();
        }

        /**
         * Delete object that matches key from cache
         * @param key        - key to remove
         * @param physically - flag, indicates whether physical removal is requried
         */
        bool remove(KeyType_ key, bool physically = true)
        {
        	bool result = false;
            boost::shared_ptr<INode> node = getNode(key);
            if(node)
                result = node->remove(physically);
            m_owner->m_lifeSpan->checkValid();

            // the main goal of result to be sure the file is removed according to requested scenario
            return result;
        }

        /**
         * try to find this item in the index to get the managed node for it
         *
         * @param item - item to find
         *
         * @return enclosing item node (if any).
         * If nothing found, invalid_argument exception is thrown
         */
        boost::shared_ptr<INode> findItem(ItemType_* item){
        	return getNode(m_getKey(item));
        }

        /** Remove all items from the index */
        void clearIndex() {
        	WriteLock lock(m_rwLock);
        	m_index.clear();
        	lock.unlock();
        }

        /** Add new item to index. Note that item is stored as a weak reference,
         * thus being deleted centralized (by Lifespan Manager or by Cache Manager)
         * it will not be available afterwards.
         *
         * @param item - new item to add (wrapped to managed node)
         *
         * @return true if item is a duplicate under the same key
         */
        bool add(boost::shared_ptr<INode> item)
        {
            KeyType_ key = m_getKey(item->value());
            WriteLock lock(m_rwLock);
            indexIterator it = m_index.find(key);
            bool duplicate = false;
            if(it != m_index.end())
            	duplicate = true;
            m_index[key] = item;
            lock.unlock();
            return duplicate;
        }

        /** Generator definitions */
        /** predicate to get start iteration point, needed for @a getNextPredicate (reentrant point) */
        typedef typename boost::function<long long()> getStartPredicate;

        /** predicate to get next item */
        typedef typename boost::function<const boost::shared_ptr<INode>(long long&, boost::shared_ptr<INode>&) > getNextPredicate;

        /** predicate to get the iteration completion guard */
        typedef typename boost::function<boost::shared_ptr<INode>()> getGuardPredicate;

        /**
         * Removes all items from index and reloads each item (this gets rid of dead nodes)
         * @return number of remained items in the index
         */
        int rebuildIndex(){
           	LOG (INFO) << "Index is near to be rebuilt.\n";
        	// reset index size:
        	size_t indexSize = 0;

        	boost::mutex* mux = m_owner->m_lifeSpan->lifespan_mux();
        	boost::lock_guard<boost::mutex> lock(*mux);

        	WriteLock lo(m_rwLock);
        	m_index.clear();
        	LOG (INFO) << "Index is cleaned up. Rebuilding...\n";
        	lo.unlock();

        	getStartPredicate start = boost::bind(boost::mem_fn(&LifespanMgr::start), m_owner->m_lifeSpan);
        	getNextPredicate next = boost::bind(boost::mem_fn(&LifespanMgr::getNextNode), m_owner->m_lifeSpan, _1, _2);
        	getGuardPredicate guard = boost::bind(boost::mem_fn(&LifespanMgr::nullNode), m_owner->m_lifeSpan);

        	// create iterator for Lifespan Manager
        	// initialize iterator start
        	boost::shared_ptr<INode> node = m_owner->m_lifeSpan->nullNode();
        	lru_gen<LifespanMgr, boost::shared_ptr<INode>, getStartPredicate, getNextPredicate, getGuardPredicate >* gen =
        			new lru_gen<LifespanMgr, boost::shared_ptr<INode>, getStartPredicate, getNextPredicate, getGuardPredicate >
        			(m_owner->m_lifeSpan, node, start, next, guard);

        	// run generator
        	for(boost::shared_ptr<INode> node; (*gen)(node);){
        	  add(node);
        	  // increase the index size
              ++indexSize;
        	}

        	return indexSize;
          }
	};

	/** I am lifespan manager and i am enumerable :) so publishing operators to be container-like */
	class LifespanMgr : public std::iterator<std::input_iterator_tag, ItemType_>{
		private:
		boost::mutex m_lifespanMgrExternalMux;      /**< mux to synchronize with Lifespan Manager operations */

		class Node;

		public:
		 /** publish sync context with Lifespan Mgr */
		boost::mutex* lifespan_mux() { return &m_lifespanMgrExternalMux; }


		/** container class used to hold nodes added within a descrete timeframe */
		struct AgeBucket {
			boost::posix_time::ptime startTime;
			boost::posix_time::ptime stopTime;
			boost::shared_ptr<Node>  first;
	     };

		private:
		std::unordered_map<long long, AgeBucket*>* m_buckets; /**< set of buckets hashed by long long representation of
		 	 	 	 	 	 	 	 	 	 	 	 	 	   * "number of hours since 1970" */
		std::vector<long long>* m_bucketsKeys;                 /**< keys of buckets ordered */

		public:

		LifespanMgr(const LifespanMgr& mgr) : m_owner(mgr._owner), m_currentBucket(mgr.m_currentBucket),
		m_buckets(mgr.m_buckets.size()), m_bucketsKeys(mgr.m_bucketsKeys.size(), m_oldestIdx(mgr.m_oldestIdx)){

			m_buckets = new std::unordered_map<long long, AgeBucket*>();

			m_numberOfBuckets.store(mgr._numberOfBuckets);
			for(std::size_t i = 0; i < mgr.m_bucketsKeys.size(); i++)
				m_bucketsKeys->push_back(mgr->m_bucketsKeys[i]);
		}

		~LifespanMgr(){
			clear();
			delete m_buckets;
		}

		private:

		/** LRUNodes is a linked list of items */
		class Node : public INode, virtual public boost::enable_shared_from_this<Node> {
		private:
			LifespanMgr*             m_mgr;       /**< associated Lifespan Manager*/
			AgeBucket*               m_ageBucket; /**< associated Age Bucket */
			boost::shared_ptr<Node>  m_next;      /**< next node */

			/** support to get the shared pointer from myself */
			boost::shared_ptr<Node> makeShared(){ return this->shared_from_this(); }

		public:
			/** construct the managed node on top of external item
			 * @param mgr   - Lifespan manager
			 * @param item  - item to store
			 */
			Node(LifespanMgr* mgr, ItemType_* item) : m_mgr(mgr), m_ageBucket(nullptr), m_next(nullPtr){
                 this->value(item);

                 long long weight = m_mgr->m_owner->tellWeight(item);
                 LOG (INFO) << "Node add : item weight = " << std::to_string(weight);
                 LOG (INFO) << "capacity before node added : "
                		 << std::to_string(m_mgr->m_owner->m_currentCapacity.load(std::memory_order_acquire)) << ".\n";
                 // Read-modify-write actions are guaranteed to read the most recently written value regardless of memory ordering
                 std::atomic_fetch_add_explicit (&m_mgr->m_owner->m_currentCapacity, weight, std::memory_order_relaxed);
                 LOG (INFO) << "capacity after node added : " <<
                		 std::to_string(m_mgr->m_owner->m_currentCapacity.load(std::memory_order_acquire)) << ".\n";
			}

			virtual ~Node() {
				LOG (INFO) << "Node destructor called" << "\n.";
			}

			/** Updates the status of the node to prevent it from being dropped from cache.
			 * If touch() is invoked on the Node creation, it should ask the Lifespan Manager to
			 * provide correct Age Bucket basing on its "timestamp". That Bucket will be the hard link host
			 * for current Node
			 *
			 * @param first - flag, indicates that node is being touched for the first time:
			 */
			bool touch(bool first = false) {
				bool valid = true;
				if( this->value() != nullptr ) {

					// first check that cache is valid to proceed with the node.
					// we do not handle touch for newly created node as well (flag "first" is set):
					if(!m_mgr->checkValid() && first){
						return false;
					}
					// ask the item about its timestamp:
					boost::posix_time::ptime timestamp = m_mgr->m_owner->tellTimestamp(this->value());
					// the following operation allows the item to control the self-promotion as an item to
					// be of the relevance, so the item itself decides how relevant should it be basing on internal conditions
					m_mgr->m_owner->updateItemTimestamp(this->value(), timestamp);

					// ask Lifespan Manager for corresponding Bucket location (if no bucket exist for this time range) or relocation:
                    AgeBucket* bucket = m_mgr->getBucketForTimestamp(timestamp);
                    if(bucket == m_ageBucket){
                    	// just do nothing, we are still in correct bucket
                    }
                    // no bucket exist, create new one:
                    if(bucket == nullptr){ // no bucket exist for specified timestamp, create one to be managed by Lifespan Mgr:

                    	LOG (INFO) << "No bucket exists for item timestamp \"" <<
                    			std::to_string(utilities::posix_time_to_time_t(timestamp)) << "\".\n";
						// share myself with Lifespan Manager:
						boost::shared_ptr<Node> sh = makeShared();

						// Acquire the corresponding bucket from Lifespan Manager.
						// This is done basing on timestamp so that it should be correct.
						boost::posix_time::ptime initial_timestamp = timestamp;
						m_ageBucket = m_mgr->openBucket(timestamp);
                        // if timestamp was changed by Lifespan Manager, update the bound item about that:
						if(initial_timestamp != timestamp){
							LOG (INFO) << "Timestamp was changed by Manager, updated : \"" <<
									std::to_string(utilities::posix_time_to_time_t(timestamp)) << "\".\n";
							m_mgr->m_owner->updateItemTimestamp(this->value(), timestamp);
						}
						// if there were no bucket acquired for the node, just do nothing. Cleanup will take care of this node later.
						if(m_ageBucket == nullptr){
							LOG (WARNING) << "No bucket was acquired for node, touch is cancelled.\n";
							return valid;
						}

						boost::mutex::scoped_lock lock(*m_mgr->lifespan_mux());
						// assign own "next" bag to be the one from the manager current bucket:
						m_next = m_ageBucket->first;
						// and assign myself to be the first one in the current bucket
						m_ageBucket->first = sh;
						lock.unlock();
                    	return valid;
                    }
                    else {
                    	if(m_ageBucket == nullptr){
                    		LOG (INFO) << "Bucket was acquired from Manager and will be used as the node bucket.\n";
                    		// assign itself to the bucket:
                    		boost::shared_ptr<Node> sh = makeShared();
                    		boost::mutex::scoped_lock lock(*m_mgr->lifespan_mux());
                    		boost::shared_ptr<Node> next = bucket->first;
                    		bucket->first = sh;
                    		// start pointing next Node from the managed bucket
                    		m_next = next;
                    		lock.unlock();
                    	}
                    	// bucket exists.
                    	// do not reallocate myself now. This will be done on cleanup.
                    	m_ageBucket = bucket;
                    }
				}
				return valid;
			}

			/** tell weight of underlying item  */
			size_t weight(){
				return m_mgr->m_owner->tellWeight(this->value());
			}

			/** set next node */
			boost::shared_ptr<Node> next(){
				return m_next;
			}

			/** get next node */
			void next(const boost::shared_ptr<Node>& node){
				if(!node)
					m_next.reset();
				else
					m_next = node;
			}

			/** get age bucket */
			AgeBucket*& bucket(){
				return m_ageBucket;
			}

            /** Removes the object from node, thereby removing it from all indexes and allows it to be RAII-deleted soon
             * @param cleanup - removal scenario, by default this is cleanup.
             * During cleanup, externally defined removal scenario is run.
             * During reload, no externally defined scenario involved, just cleaning local structures
             */
            bool remove(bool cleanup)
            {
            	bool result = false;
                if( m_ageBucket != nullptr && this->value() != nullptr ){
                	long long weight = m_mgr->m_owner->tellWeight(this->value());

                	// below will run external deletion in either cleanup or reload mode, basing on the flag arrived:
                	try{
                		result = m_mgr->m_owner->deleteItemExt(this->value(), cleanup);
                	}
                	catch(...){
                		LOG (WARNING) << "Exception thrown from external deleter." << "\n";
                	}
                    if(!result){
                    	LOG (WARNING) << "Node deletion is requested for item that cannot be removed. Node will not be removed as well.\n";
                    	return result;
                    }

                	// say no external value is managed more by this node
                    this->value(nullptr);

                    LOG (INFO) << "capacity before node removal : " <<
                    		std::to_string((m_mgr->m_owner->m_currentCapacity.load(std::memory_order_acquire)) ) << "\n";
                    // decrease cache current capacity once the node is removed
                	std::atomic_fetch_sub_explicit (&m_mgr->m_owner->m_currentCapacity, weight, std::memory_order_relaxed);
                    LOG (INFO) << "capacity after node removal : " <<
                    		std::to_string( (m_mgr->m_owner->m_currentCapacity.load(std::memory_order_acquire)) ) << "\n";
                	// decrease number of hard items
                    std::atomic_fetch_sub_explicit (&m_mgr->m_owner->m_numberOfHardItems, 1u, std::memory_order_relaxed);
                }
                else
                	LOG (WARNING) << "LRU Node : Node content removal was not done as expected by scenario due to leak of metadata" << "\n";
                return result;
            }
		};

        LRUCache<ItemType_>* m_owner;                       /**< Lifespan owner - the Cache */

        const int            _checkOnceInMinutes = 10;      /**< rhythm, in minutes, of validity check routine invocation (not less than) */
        const int            _defaultTimeSliceInHours = 6;  /**< default time slice */

        const int m_numberOfBucketsLimit = 5000;            /**< supported number of buckets */
        mutable std::atomic<unsigned> m_numberOfBuckets;    /**< current number of buckets */

        boost::posix_time::ptime         m_checkTime;       /**< next time to check the cache for validity */
        boost::posix_time::time_duration m_timeSlice;       /**< time slice to introduce Age Bags*/

        boost::posix_time::ptime m_startTimestamp;         /**< the start point to plan the Buckets, no items older than this value will be accepted **/
        long long                m_oldestIdx;              /**< representation of oldest timestamp index, measurement unit is hour. An index is created per
         	 	 	 	 	 	 	 	 	 	 	 	 	*   _timeSlice hours by default */

        AgeBucket* m_currentBucket;                         /**< current opened bucket */

        typedef typename std::unordered_map<long long, AgeBucket*>::iterator bagsIter;

        /** check that indexes are valid and rebuild them if getting too big indexes */
        void checkIndexValid(){
        	// if indexes are getting too big its time to rebuild them
        	unsigned soft_items_fact = m_owner->m_numberOfSoftItems.load(std::memory_order_acquire);
        	unsigned hard_items_fact = m_owner->m_numberOfHardItems.load(std::memory_order_acquire);

        	LOG (INFO) << "Checking whether index is valid. Soft items = \"" << std::to_string(soft_items_fact) << "\";" <<
        			"hard items : \"" << std::to_string(hard_items_fact) << "\"; max limit forbidden items = \"" <<
        			std::to_string(m_owner->_max_limit_of_forbidden_items) << "\".";

        	// if limit of forbidden nodes is reached, start re-indexing
        	if( soft_items_fact - hard_items_fact >= m_owner->_max_limit_of_forbidden_items ){
            	LOG (INFO) << "Check index validation is triggered. Soft items = \"" << std::to_string(soft_items_fact) << "\";" <<
            			"hard items : \"" << std::to_string(hard_items_fact) << "\".";

        		// go over indexes and note their capacity
        		for(auto it = m_owner->m_indexList->begin(); it != m_owner->m_indexList->end(); ++it){
        			soft_items_fact = it->second->rebuildIndex();
        		}
        		hard_items_fact = soft_items_fact;

        		// update LRU Cache statistics collected for Indexes after re-indexing.
        		// RMW should always read last value, so that using relaxed memory
        		std::atomic_exchange_explicit(&m_owner->m_numberOfSoftItems, soft_items_fact, std::memory_order_relaxed);
        		std::atomic_exchange_explicit(&m_owner->m_numberOfHardItems, hard_items_fact, std::memory_order_relaxed);
        	}
        }

        /** calculates buckets set index basing on specified timestamp
         * @param timestamp - timestamp to get the key for
         *
         * @return index within the set of buckets
         */
        long long timestamp_to_key(boost::posix_time::ptime timestamp){
        	// how much hours remained since 1970?
        	boost::posix_time::ptime epoch = boost::posix_time::time_from_string("1970-01-01 00:00:00.000");
        	boost::posix_time::time_duration const diff = timestamp - epoch;

        	// calculate index in array of buckets that fit requested timestamp:
        	long long idx = m_oldestIdx + (diff.hours() - m_oldestIdx) / m_timeSlice.hours();
        	return idx;
        }

		public:
        /**
         * Construct Lifespan Manager for cache @a owner basing on specified @a timeSilce
         *
         * @param owner     - LRU Cache
         * @param startFrom - timestamp which specify the cache "start from" timestamp.
         * Any item with age older than timestamp will not be accepted.
         * Internal Buckets planning starts from this point.
         *
         * @param timeSlice - time slice to use for Buckets planning. Default is "non-specified",
         * in that case it will be assigned to 6 hours automatically
         */
        LifespanMgr(LRUCache<ItemType_>* owner, boost::posix_time::ptime startFrom, boost::posix_time::time_duration timeSlice = boost::posix_time::hours(-1)) :
        			m_numberOfBuckets(0), m_startTimestamp(startFrom), m_currentBucket(nullptr) {
        	LOG (INFO) << "Lifespan manager : start timestamp : \"" << std::to_string(utilities::posix_time_to_time_t(m_startTimestamp)) <<
        			"\"\n";
        	m_owner = owner;
        	if(timeSlice.is_negative())
        		m_timeSlice = boost::posix_time::hours(_defaultTimeSliceInHours);
        	else
        		m_timeSlice = timeSlice;

        	// calculate next timestamp to perform the cache check:
        	boost::posix_time::ptime now = boost::posix_time::microsec_clock::local_time();
         	m_checkTime = now + boost::posix_time::minutes(_checkOnceInMinutes);

         	// calculate oldest index in the cache
        	boost::posix_time::ptime epoch = boost::posix_time::time_from_string("1970-01-01 00:00:00.000");
         	boost::posix_time::time_duration const diff = m_startTimestamp - epoch;

         	m_oldestIdx = diff.hours();

         	m_buckets = new std::unordered_map<long long, AgeBucket*>();
         	m_bucketsKeys = new std::vector<long long>();

         	openBucket(startFrom);
         }

        /** reload the lifespan manager with a given start time
         * @param startFrom - start timestamp, the oldest allowed timestamp within the manager registry
         */
        void reload(boost::posix_time::ptime startFrom){

        	m_startTimestamp = startFrom;
        	// calculate oldest index in the cache
        	boost::posix_time::ptime epoch = boost::posix_time::time_from_string("1970-01-01 00:00:00.000");
         	boost::posix_time::time_duration const diff = m_startTimestamp - epoch;

         	m_oldestIdx = diff.hours();

         	openBucket(startFrom);
        }
        /** lookup for Age Bucket to fit specified timestamp
         * @param timestamp - timestamp to get the Bucket for
         *
         * @return Age Bucket if one found, nullptr otherwise
         */
        AgeBucket* getBucketForTimestamp(boost::posix_time::ptime timestamp){
        	// if there's ancient timestamp specified, reply no bucket exists:
        	if(timestamp < m_startTimestamp){
        		LOG (INFO) << "Timestamp is too old to get the bucket for : \"" <<
        				std::to_string(utilities::posix_time_to_time_t(timestamp)) << "\". Min timestamp : \"" <<
        				std::to_string(utilities::posix_time_to_time_t(m_startTimestamp)) << "\".\n";
        		return nullptr;
        	}

        	// calculate index in array of buckets that fit requested timestamp:
        	long long idx = timestamp_to_key(timestamp);
    		LOG (INFO) << "Getting bucket with a key \"" << std::to_string(idx) << "\" for timestamp \"" <<
    				std::to_string(utilities::posix_time_to_time_t(timestamp)) << "\". \n";
            auto it = std::find(m_bucketsKeys->begin(), m_bucketsKeys->end(), idx);
            if(it == m_bucketsKeys->end())
            	return nullptr;

            LOG (INFO) << "Key \"" << std::to_string(idx) << "\" exists for for timestamp \"" <<
                				std::to_string(utilities::posix_time_to_time_t(timestamp)) << "\". \n";
            // key was found, get the bucket for it:
            return (*m_buckets)[(*it)];
        }

        boost::shared_ptr<INode> add(ItemType_* value)
        {
        	// wrap the item to managed Node
        	boost::shared_ptr<Node> sp(new Node(this, value));
        	// and touch it to mark as active and move to corresponding Lifespan Manager Age bucket.
        	// specify the "true" flag for touch options.
            bool added = sp->touch(true);
            if(added)
            	return sp;
            // remove physically
            sp->remove(true);
            return nullPtr;
        }

        /** checks to see if cache is still valid and if LifespanMgr needs to do maintenance
         * @return validation status. True if cache is valid.
         */
        bool checkValid(){
        	boost::posix_time::ptime now = boost::posix_time::microsec_clock::local_time();
        	bool valid = true;

        	long long currentCapacity = m_owner->m_currentCapacity.load(std::memory_order_acquire);

        	// If lock is currently acquired, just skip and let next touch() perform the cleanup.
        	if((now > m_checkTime) || (currentCapacity >= m_owner->m_capacityLimit))
        	{
        		boost::mutex::scoped_lock lock(*lifespan_mux(), boost::try_to_lock);
        		if(lock){
        			// get the current capacity once again (another thread may issued cleanup till now)
        			currentCapacity = m_owner->m_currentCapacity.load(std::memory_order_acquire);
        			if((now > m_checkTime) || (currentCapacity > m_owner->m_capacityLimit)){
        				// if cache is no longer valid throw contents away and start over, else cleanup old items
        				if( m_numberOfBuckets > m_numberOfBucketsLimit || (m_owner->m_isValid && !m_owner->m_isValid()) ){
        					// unlock lifespan manager so far and let it run cleanup
        					lock.unlock();
        					m_owner->clear();
        				}
        				else{
        					lock.unlock();
        					valid = cleanUp(now);
        				}
        			}
        		}
        		// check is completed. Update next cache check timestamp:
        		now = boost::posix_time::microsec_clock::local_time();
        		m_checkTime = now + boost::posix_time::minutes(_checkOnceInMinutes);
        	}
        	return valid;
        }

        /** Remove old items or items beyond capacity from LifespanMgr.
         *  Note - since we do not physically move items when touched we must check items in bag to determine if they should be deleted.
         *  or moved.  Also Nodes that were removed by setting value to null get removed now.  Removing an item from LifespanMgr allows
         *  them to be cleared from index later.  If removed item is retrieved by index where weak references are stored, it will be re-added to LifespanMgr.
         *
         *  Note: this routine has no internal lock, therefore should be called in the guarded context
         *
         *  @return cleanup operation success. Cleanup is failed if required amount of space was not freed
         */
        bool cleanUp(boost::posix_time::ptime now)
        {
        	// cleanup will be called only if the cache capacity overflows and will affect the oldest Age Bucket
        	// and more buckets if needed.

        	long long currentCapacity = m_owner->m_currentCapacity.load(std::memory_order_acquire);
        	long long weightToRemove = currentCapacity - m_owner->m_capacityLimit;

        	LOG (INFO) << "LRU Cleanup is triggered. Current capacity = " << std::to_string(currentCapacity) <<
        			". Weight to remove = " << std::to_string(weightToRemove) << "; capacity limit = " <<
        			std::to_string(m_owner->m_capacityLimit) << ".\n";

        	LOG (INFO) << "LRU Cleanup : buckets number = " << std::to_string(m_bucketsKeys->size()) << ".\n";

        	boost::mutex::scoped_lock lock(*lifespan_mux());

        	bool cleanupSucceed = false;

        	auto it = m_bucketsKeys->begin();

        	// go over buckets, from very old to newer, until the necessary cleanup is done:
        	while( weightToRemove > 0 && it != m_bucketsKeys->end()) {
            	// get the key to describe the oldest bucket:
            	long long key = (*(it));

            	// get the oldest bucket:
            	AgeBucket* bucket = (*m_buckets)[key];
            	LOG (INFO) << "Bucket is retrieved for key \"" << std::to_string(key) << "\".\n";
                bool deletePermitted = true;

                if(bucket == nullptr){
                	LOG (ERROR) << "No bucket with a key \"" << std::to_string(key) << "\" within buckets collection while key exists.\n";
                	break;
                }

                // go over nodes under this bucket:
        		boost::shared_ptr<Node> node = bucket->first;
        		LOG (INFO) << "First node is retrieved for bucket with a key \"" << std::to_string(key) << "\".\n";
        		if(node){
        			LOG (INFO) << "First node exists for bucket with a key \"" << std::to_string(key) << "\".\n";
        		}
        		// handle the situation when there's single node in the bucket and the bucket is the recent one,
        		// so, the cleanup was triggered by adding the node which is near to be deleted right now (suppress this).
        		// cleanup will be triggered next time and remove this node without affect of possible current usage
        		if((m_bucketsKeys->size() == 1) && node && !node->next()){
        			LOG (WARNING) << "There's only one bucket exists with a single node added, nothing to remove for bucket with a key \""
        					<< std::to_string(key) << "\" .\n";
        			it++;
        			continue;
        		}
        		if(!node){
        			LOG (WARNING) << "Empty bucket detected with a key \"" << std::to_string(key) << "\" .\n";
        			it++;
        			continue;
        		}

        		// store the current alive node within the cleaned up bucket (suppose the oldest bucket is still active):
        		boost::shared_ptr<Node> active = nullPtr;
        		boost::shared_ptr<Node> head = nullPtr;

        		// and reverse nodes under this bucket so that most recent added will be last to delete:
        		LOG (INFO) << "Going to reverse nodes list under bucket with a key \"" << std::to_string(key) << "\".\n";
        		utilities::reverse(node);
    			LOG (INFO) << "Bucket content is reversed to start from oldest items for bucket with a key \"" <<
    					std::to_string(key) << "\".\n";

                while(node && (weightToRemove > 0)){

        			// note the node next to current one
        			boost::shared_ptr<Node> next = node->next();

        			if( node->value() != nullptr && node->bucket() != nullptr ){
        				if( node->bucket() == bucket ) {
        					// item has not been touched since bucket was closed, so remove it from LifespanMgr if it is allowed for removal.
                            if(!m_owner->markForDeletion(node->value())){
                            	// no approval for item removal received. Deny the age bucket removal
                            	deletePermitted = false;

                            	if(active){
                            		active->next(node);
                            		active = active->next();
                            	}
                            	else {
                            		active = node;
                            		head = active;
                            	}

                            	// try next node:
                            	node = next;
                            	continue;
                            }
        					// get the weight the item will release back to the cache:
        					long long toRelease = m_owner->tellWeight(node->value());

                            // remove the node
                		    bool result = node->remove(true);
                		    if(!result){
                		    	LOG (WARNING) << " Cleanup scenario : Node content was not cleaned up as expected by scenario" << "\n";
                            	if(active){
                            		active->next(node);
                            		active = active->next();
                            	}
                            	else {
                            		active = node;
                            		head = active;
                            	}

                		    	// try next node:
                            	node = next;
                		    	continue;
                		    }

                		    weightToRemove -= toRelease;
                		    LOG (INFO) << "Cleanup : to remove = " << std::to_string(weightToRemove) << std::endl;

                		    // cut off it from registry
                			node.reset();
                			if(active)
                				// and say active yet not point to any node
                				active->next(nullPtr);
        				}
        				else {
        					// item has been touched and should be moved to correct age bag now
        					node->next(node->bucket()->first);
        					// and point another Age Bucket to this node as to the first node:
        					node->bucket()->first = node;
        				}
        			}
        			node = next;
        		}

        		if(!deletePermitted){
        			LOG (WARNING) << "Cache bucket \"" << std::to_string(key) << "\" is not deleted as its content is still in use.\n";

        			// reverse the remained list of nodes under this bucket back:
        			utilities::reverse(head);
        			head->bucket()->first = head;
        			it++;
        			continue; // go next bucket if current bucket deletion is denied (as its node is restricted from deletion externally)
        		}

        		// if bucket still has nodes but required space is freed, break the cleanup
        		if(node && (weightToRemove <= 0)){
        			LOG (INFO) << "Cache bucket \"" << std::to_string(key) << "\" still has alive nodes. Required space is freed.\n";
        			utilities::reverse(node);
        			node->bucket()->first = node;
        			break;
        		}

        		LOG (INFO) << "Cache bucket \"" << std::to_string(key) << "\" is cleaned up completely. Will be deleted from cache.\n";
        		// if the bucket was cleaned up completely - remove the bucket.
        		// drop the bucket from set of buckets:
        		m_buckets->erase(key);
        		// delete the non-needed bucket:
        		delete bucket;
        		// drop the key from key list:
        		it = m_bucketsKeys->erase(it);

        		LOG (INFO) << "Cache bucket \"" << std::to_string(key) << "\" is deleted from cache.\n";

        		if ( std::atomic_fetch_sub_explicit (&m_numberOfBuckets, 1u, std::memory_order_release) == 0u ) {
        			std::atomic_thread_fence(std::memory_order_acquire); // all buckets were cleaned up
        			break;
        		}

        	}
        	lock.unlock();
        	if(weightToRemove <= 0)
        		cleanupSucceed = true;

        	checkIndexValid();
        	return cleanupSucceed;
        }

        /** Remove all items from LifespanMgr and reset */
        void clear() {
        	boost::mutex::scoped_lock lock(*lifespan_mux());
        	LOG(INFO) << "buckets size : " << std::to_string(m_buckets->size()) << std::endl;
         	for(bagsIter it = m_buckets->begin(); it != m_buckets->end(); it++){
        		boost::shared_ptr<Node> node = it->second->first;
        		while(node){
        			boost::shared_ptr<Node> next = node->next();
        			// remove the node, specify the scenario is reload so that the item will not be removed externally
        			node->remove(false);
        			node.reset();
        			node = next;
        		}
        		delete it->second;
        	}
         	// clear buckets collection:
         	m_buckets->clear();

         	// clear keys collection:
         	m_bucketsKeys->clear();

         	m_currentBucket = nullptr;
        	lock.unlock();
        	// reset item counters
            std::atomic_exchange_explicit(&m_owner->m_currentCapacity, 0ll, std::memory_order_relaxed);
            std::atomic_exchange_explicit(&m_owner->m_numberOfHardItems, 0u, std::memory_order_relaxed);
            std::atomic_exchange_explicit(&m_owner->m_numberOfSoftItems, 0u, std::memory_order_relaxed);
        }

        /** get ready a new AgeBucket for usage. Close the previous one
         * @param start - start time for new bucketadd
         *
         * @return constructed bucket
         */
        AgeBucket* openBucket(boost::posix_time::ptime& start)
        {
        	boost::mutex* mux = lifespan_mux();
        	boost::mutex::scoped_lock lock(*mux);

        	// close last age bag
        	if( m_currentBucket != nullptr )
        		m_currentBucket->stopTime = start;

        	// create the key for this bucket
        	long long idx = timestamp_to_key(start);
        	LOG (INFO) << "New bucket is requested with a key \"" << std::to_string(idx) << "\".\n";
        	// check for overflow and do not proceed if broken timestamp was received as we rely on it to be correct
        	if(idx < 0){
        		// assign the timestamp to the node explicitly to "now":
        		start = boost::posix_time::microsec_clock::local_time();
        		idx = timestamp_to_key(start);
        	}
        	LOG (INFO) << "Going to construct new bucket with a key \"" << std::to_string(idx) << "\".\n";
        	// open new age bag for next time slice
        	AgeBucket* newBucket = new AgeBucket();

        	LOG (INFO) << "New bucket is constructed for key \"" << std::to_string(idx) << "\".\n";

        	std::pair<long long, AgeBucket*> bucket_pair (idx, newBucket);
        	LOG (INFO) << "New bucket is going to be added to registry for key \"" << std::to_string(idx) << "\".\n";
        	m_buckets->insert(bucket_pair);
        	LOG (INFO) << "New bucket key \"" << std::to_string(idx) << "\" is going to be stored.\n";
        	m_bucketsKeys->push_back(idx);

        	LOG (INFO) << "Bucket keys size : \"" << std::to_string(m_bucketsKeys->size()) << "\". Number of buckets = \"" <<
        			m_buckets->size() << "\"\n";

        	newBucket->startTime = start;
        	newBucket->first = nullPtr;

            // increment number of buckets:
            std::atomic_fetch_add_explicit (&m_numberOfBuckets, 1u, std::memory_order_relaxed);

        	// say current bucket is a new one:
        	m_currentBucket = newBucket;
        	return newBucket;
        }

        boost::shared_ptr<INode> nullNode() { return nullPtr; }

        /** reply with first key for buckets set */
        long long start(){
        	return 0;
        }

        /** get next buckets set key using sequence number */
        const long long getNextKey(long long& idx){
        	return (*m_bucketsKeys)[idx++];
        }

        /** get next node in the Lifespan Registry. Traverse all buckets, from most recent to most ancient */
        const boost::shared_ptr<INode> getNextNode(long long& idx, boost::shared_ptr<INode>& currentNode){
        	AgeBucket* bucket;
        	boost::shared_ptr<INode> ret;

        	long long key = (*m_bucketsKeys)[idx];
        	if((bucket = (*m_buckets)[key]) == nullptr)
        		return nullNode();

        	// if no node specified
        	if(!currentNode){
        		return bucket->first;
        	}
        	// downcast current node to internal type:
            boost::shared_ptr<Node> internalCurrent = boost::dynamic_pointer_cast<Node>(currentNode);
        	if(internalCurrent->next()){ // if there's something next exists
        		if(internalCurrent->next()->value() != nullptr)
        			return internalCurrent->next();
        		else{
        			return nullNode();
        		}
        	}
        	else{ // no node next to current
        		if(key == m_oldestIdx) // end of collection reached, send finalization marker
        			return nullNode();
        		else{
        			// go next bucket
        			key = getNextKey(idx);
        			currentNode.reset();
        			return getNextNode(idx, currentNode);
        		}
        	}
        	return ret;
        }
	};


	/**************************************** Members **********************************************/

	LifespanMgr* m_lifeSpan;
	std::unordered_map<std::string, IIndexInternal* >*  m_indexList;  /**< set of defined indexes */

	mutable std::atomic<unsigned>  m_numberOfHardItems;  /**< number of hard items - really hosted by Cache right now */
	mutable std::atomic<unsigned>  m_numberOfSoftItems;  /**< number of soft items - have ever been added into the cache since last indexes clean.
	                                                           Soft means that this amount includes either deleted nodes and existing.
	                                                           For Indexes cleanup scenario  */

	const unsigned _max_limit_of_forbidden_items = 200;  /**< limit of forbidden items in particular Index. Forbidden = deleted from cache node */

	/** external call to get the item weight */
    long long tellWeight(ItemType_* item){
    	if(m_tellWeightPredicate)
    		return m_tellWeightPredicate(item);
    	return -1;
    }

    /** predicate to get the item timestamp */
    boost::posix_time::ptime tellTimestamp(ItemType_* item){
    	if(m_tellItemTimestamp)
    		return m_tellItemTimestamp(item);
    	return boost::posix_time::microsec_clock::local_time();
    }

    /** external call to get the item deletion approval */
    bool markForDeletion(ItemType_* item){
    	if(m_markForDeletion)
    		return m_markForDeletion(item);
    	return true;
    }

    /** run externally defined scenario "on item deleted"
     * @param item       - item to remove from cache
     * @param physically - flag, indicates whether the item should be removed physically from source
     */
    bool deleteItemExt(ItemType_* item, bool physically = true){
    	if(m_itemDeletionPredicate)
    		return m_itemDeletionPredicate(item, physically);
    	return false;
    }

    void updateItemTimestamp(ItemType_* item, const boost::posix_time::ptime& timestamp){
    	if(m_acceptAssignedTimestamp)
    		m_acceptAssignedTimestamp(item, timestamp);
    }

    /** Add an item to the cache.
     * Note that in case of duplicate this routine will overwrite the @a item pointer
     * to the existing one.
     *
     * @param [in]  item      - item to add
     * @param [out] succeed   - flag, indicates whether the item is in the registry
     * @param [out] duplicate - flag, indicates whether the item is duplicate by key
     */
    boost::shared_ptr<INode> addInternal(ItemType_*& item, bool& succeed, bool& duplicate) {
    	succeed = false;
    	if( item == nullptr )
    		return nullPtr;

         // see if item is already in index
         boost::shared_ptr<INode> node = nullPtr;

         for(auto idx : (*m_indexList)){
        	 if((node = idx.second->findItem(item)))
        		 break;
         }

         // duplicate is prevented from being added into the cache
         duplicate = (node && (*node->value() == (*item)));
         if( duplicate ){
        	 LOG(WARNING) << "Duplicate found within the registry.\n";
        	 // reassign the link just in case is somebody uses it now outside
        	 ItemType_* _item = item;
        	 item = node->value();
        	 // delete the reference to the item requested to add
        	 delete _item;
        	 succeed = true;
        	 return node;
         }

         node = m_lifeSpan->add(item);
         if(!node){
        	 // Unable to add new node, the cache is full, and no items could be removed to free space enough
        	 LOG (WARNING) << "new node could not be added into the cache, reason : no free space available.\n";
        	 succeed = false;
        	 return node;
         }

         // make sure node gets inserted into all indexes
         for(auto item : (*m_indexList)){
        	 item.second->add(node);
         }

         // whenever we add new item, we increment both hard and soft items amount:
         std::atomic_fetch_add_explicit (&m_numberOfHardItems, 1u, std::memory_order_relaxed);
         std::atomic_fetch_add_explicit(&m_numberOfSoftItems, 1u, std::memory_order_relaxed);

         succeed = true;
         return node;
     }

	protected:
    boost::posix_time::ptime        m_startTime;          /**< cache oldest item timestamp */

	public:

	/** construct the LRU cache object
	 *
	 * @param startFrom - timestamp, create the start time point in order to accept only items with later timestamps into the cache
	 * @param isValid   - predicate used to determine if cache is out of date.  Called before index access
	 *
	 */
    LRUCache(boost::posix_time::ptime startFrom,  long long capacity, isValidPredicate isValid = 0) : m_startTime(startFrom){

    	m_capacityLimit = capacity;

        m_isValid  = isValid;
        m_lifeSpan = new LifespanMgr(this, startFrom);

        // statistics collected for cache cleanup
        m_currentCapacity.store(0l);

        // statistics collected for indexes cleanup (to get the rid of dead nodes)
        m_numberOfHardItems.store(0u);
        m_numberOfSoftItems.store(0u);

        m_indexList = new std::unordered_map<std::string, IIndexInternal*>();
    }

    /** cleanup */
    virtual ~LRUCache() {
    	if(m_indexList == nullptr || m_lifeSpan == nullptr)
    		return;

    	for(auto item : (*m_indexList)){
    		delete item.second;
       	}

    	// delete indexes list:
    	if(m_indexList != nullptr)
    		delete m_indexList;
    	m_indexList = nullptr;

    	if(m_lifeSpan != nullptr)
    		delete m_lifeSpan;

    	m_lifeSpan = nullptr;

    	if(m_indexList != nullptr)
    		delete m_indexList;
    	m_indexList = nullptr;
    }

    /** Retrieve a index by name */
    template<typename KeyType_>
    IIndex<KeyType_> getIndex(std::string indexName)
    {
        IIndexInternal* index;
        auto it = m_indexList->find(indexName);
        return it != m_indexList->end() ? (*it) : nullptr;
    }

    /// <summary>Retrieve a object by index name / key</summary>
    template<typename KeyType_>
    ItemType_ getValue(std::string indexName, KeyType_ key)
    {
        IIndex<KeyType_> index = getIndex<KeyType_>(indexName);
        return (index == nullptr ? nullptr : index[key]);
    }

    /** Add a new index to the cache
     *
     * @tparam KeyType_ - the type of the key value
     *
     * @param indexName 	- the name to be associated with this list
     * @param getKey    	- predicate to get key from object
     * @param loadItem      - predicate to load object if it is not found in index
     * @param constructItem - predicate to construct the object to be hosted by Cache by index
     *
     * @return newly created index
     */
    template<typename KeyType_>
    IIndex<KeyType_>* addIndex(std::string indexName, GetKeyFunc<KeyType_> getKey, LoadItemFunc<KeyType_> loadItem,
    		ConstructItemFunc<KeyType_> constructItem)
    {
        Index<KeyType_>* index = new Index<KeyType_>(this, getKey, loadItem, constructItem);
        (*m_indexList)[indexName] = index;
        return index;
    }

    /** Add an item to the cache (not needed if accessed by index) */
    bool add(ItemType_*& item, bool& duplicate)
    {
    	// cannot add an item to the cache if the capacity limit exceeded (for example, all cache content is still in use):
    	/*
    	if(m_currentCapacity.load(std::memory_order_acquire) > m_capacityLimit){
    		LOG (WARNING) << "Item is not added to the cache as capacity limit exceeded. Capacity = " << std::to_string(m_currentCapacity) <<
    				"; limit = " << std::to_string(m_capacityLimit) << "\n";
    		return false;
    	}
    	*/
    	bool success = false;
    	duplicate    = false;

    	// items that are issued earlier than specified in m_startTime are rejected as well as null-items:
    	if(item == nullptr || (m_tellItemTimestamp && m_tellItemTimestamp(item) < m_startTime)){
    		LOG (WARNING) << "File creation time is older than the cache start timestamp, this item will not be tracked.\n";
    		return success;
    	}

        addInternal(item, success, duplicate);
        return success;
    }

    /** Remove all items from cache */
    void clear(){
    	if(m_indexList == nullptr || m_lifeSpan == nullptr)
    		return;

        // cleanup indexes
    	for(auto item : (*m_indexList)){
    		 item.second->clearIndex();
       	}
    	// cleanup lifespan manager registry
    	m_lifeSpan->clear();
    }

    /** reset start time and reload lifespan manager accordingly,
     *  to avoid the situation when lifespan manager contains nodes older than
     *  new start time
     *
     *  @param start - new start time, minimum timestamp required for cache item to be
     *  the part of the current cache
     */
    void resetStartTime(boost::posix_time::ptime start){
    	m_startTime = start;
    	m_lifeSpan->reload(start);
    }
};
}

#endif /* LRU_CACHE_HPP_ */
