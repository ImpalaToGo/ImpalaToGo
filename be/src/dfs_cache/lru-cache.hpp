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
#include "common/logging.h"

namespace impala{

/** Giving the boost::shared_ptr<T> to nothing (nullptr) */
class {
public:
    template<typename T>
    operator boost::shared_ptr<T>() { return boost::shared_ptr<T>(); }
} nullPtr;

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
	using LoadItemFunc = typename boost::function<ItemType_*(KeyType_ key)>;

	/** "get capacity limit" predicate */
	using TellCapacityLimitPredicate = typename boost::function<size_t()>;

	/** "get item weight" predicate */
	using TellWeightPredicate = typename boost::function<long long(ItemType_* item)>;

	/** "check item can be removed" predicate.
	 * @param item - item to check for removal approval
	 *
	 * @return bool if item is allowed if removal */
	using TellItemIsIdle = typename boost::function<bool(ItemType_* item)>;

	/** "get the item timestamp" predicate */
	using TellItemTimestamp = typename boost::function<boost::posix_time::ptime(ItemType_* item)>;

	/** "item deletion" external call predicate, its up to implementor what to do with the item when it is deleted from the cache */
	using ItemDeletionPredicate = typename boost::function<bool(ItemType_* item)>;

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

     	virtual void touch()    = 0;  /** method that marks the item as "accessed" */
        virtual void remove()   = 0;  /** remove the node */
        virtual size_t weight() = 0;  /** tell weight of underlying item*/

     };

protected:
    isValidPredicate           m_isValid;                     /**< predicate to be invoked to check for vaidity */
    TellCapacityLimitPredicate m_tellCapacityLimitPredicate;  /**< predicate to be invoked to get the limit of capacity. For capacity planning */
    TellWeightPredicate        m_tellWeightPredicate;         /**< predicate to be invoked to get the weight of item. For cleanup planning */
    TellItemIsIdle             m_tellItemIsIdle;              /**< predicate to check whether item can be removed. */
    TellItemTimestamp          m_tellItemTimestamp;           /**< predicate to tell item timestamp */
    ItemDeletionPredicate      m_itemDeletionPredicate;       /**< predicate to run externally when the item is removed from the cache */

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
    	 * @param key - key to find
    	 *
    	 * @return the value of object associated with the cache
    	 */
    	virtual ItemType_ & operator [](KeyType_ key) = 0;

    	/**
    	 * Delete object that matches key from cache
    	 *
    	 * @param key - key to remove from cache
    	 */
    	virtual void remove(KeyType_ key) = 0;
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
         * @param owner    - bound LRU cache
         * @param getKey   - "get key" predicate
         * @param loadItem - "load item" predicate (if it was not found within an index)
         */
        Index(LRUCache<ItemType_>* owner, GetKeyFunc<KeyType_>& getKey, LoadItemFunc<KeyType_>& loadItem)
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
            rebuildIndex();
        }

        /** Index getter
         * @param key - key to find
         *
         * @return node underlying value-item. If no node exists, rise and invalid_argument exception
         */
        const ItemType_& operator [](KeyType_ key) const{
        	INode* node = getNode(key);

        	if(!node){
        		// if autoload is configured, invoke it to get the item into the cache
        		if(!m_loadItem)
        			throw std::invalid_argument("no key present");
        		// autoload is configured
        		node = m_owner->add(m_loadItem(key));
        		if(!node)
        			throw std::invalid_argument("no key present");
        	}
        	// check node value. It may be lazy-erased
        	if(node->value() == nullptr)
        		throw std::invalid_argument("no value associated with a node (erased)");

        	node->touch();
        	return *(node->value());
        }

        /** Index getter
         *  @param key - key to find
         *
         *  @return node underlying value-item. If no node exists, rise and invalid_argument exception
         */
        ItemType_ & operator [](KeyType_ key){
        	boost::shared_ptr<INode> node = getNode(key);

        	if(!node){
        		// if autoload is configured, invoke it to get the item into the cache
        		if(!m_loadItem)
        			throw std::invalid_argument("no key present");
        		// autoload is configured
        		node = m_owner->addInternal(m_loadItem(key));
        		if(!node)
        			throw std::invalid_argument("no key present");
        	}
        	// check node value. It may be lazy-erased
        	if(node->value() == nullptr)
        		throw std::invalid_argument("no value associated with a node (erased)");

        	node->touch();
        	return *(node->value());
        }

        /**
         * Delete object that matches key from cache
         * @param key - key to remove
         */
        void remove(KeyType_ key)
        {
            boost::shared_ptr<INode> node = getNode(key);
            if(node)
                node->remove();
            m_owner->m_lifeSpan->checkValid();
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
        	size_t indexSize = 0;

        	boost::mutex* mux = m_owner->m_lifeSpan->lifespan_mux();
        	boost::lock_guard<boost::mutex> lock(*mux);

        	WriteLock lo(m_rwLock);
        	m_index.clear();

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
        	  // ask the node about its weight to recalculate index size
              indexSize += node->weight();
        	}

        	lo.unlock();
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
                 std::atomic_fetch_add_explicit (&m_mgr->m_owner->m_currentCapacity, weight, std::memory_order_relaxed);
			}

			/** destructs the bound resource which is originally supplied as a pointer */
			virtual ~Node() {
				//if()
				std::cout << "Node destructor called" << std::endl;
			}

			/** Updates the status of the node to prevent it from being dropped from cache.
			 * If touch() is invoked on the Node creation, it should ask the Lifespan Manager to
			 * provide correct Age Bucket basing on its "timestamp". That Bucket will be the hard link host
			 * for current Node
			 */
			void touch() {
				if( this->value() != nullptr ) {
					// Have a new timestamp! So that should switch the bucket where located:
					boost::posix_time::ptime timestamp = m_mgr->m_owner->tellTimestamp(this->value());
					// ask Lifespan Manager for corresponding Bucket location (if no bucket exist for this time range) or relocation:

                    AgeBucket* bucket = m_mgr->getBucketForTimestamp(timestamp);
                    if(bucket == m_ageBucket){
                    	// just do nothing, we are still in correct bucket
                    }
                    // no bucket exist, create new one:
                    if(bucket == nullptr){ // no bucket exist for specified timestamp, create one to be managed by Lifespan Mgr:
						// share myself with Lifespan Manager:
						boost::shared_ptr<Node> sh = makeShared();
                    	boost::mutex::scoped_lock lock(*m_mgr->lifespan_mux());
                    	m_mgr->openBucket(timestamp);

                        // assign own "next" bag to be the one from the manager current bucket:
                    	m_next = m_mgr->m_currentBucket->first;
                        // and assign myself to be the first one in the current bucket
                        m_mgr->m_currentBucket->first = sh;
                        m_ageBucket = m_mgr->m_currentBucket;
                        lock.unlock();

                        //std::atomic_fetch_add_explicit (&m_mgr->_owner->m_currentCapacity, 1l, std::memory_order_relaxed);
                    	return;
                    }
                    else {
                    	if(m_ageBucket == nullptr){
                    		// assign itself to the bucket:
                    		boost::shared_ptr<Node> sh = makeShared();
                    		boost::shared_ptr<Node> next = bucket->first;
                    		bucket->first = sh;
                    		// start pointing next Node from the managed bucket
                    		m_next = next;
                    	}
                    	// bucket exists.
                    	// do not reallocate myself now. This will be done on cleanup.
                    	m_ageBucket = bucket;
                    }
                    m_mgr->checkValid();
				}
			}

			/** tell weight of underlying item  */
			size_t weight(){
				return m_mgr->m_owner->tellWeight(this->value());
			}

			/** get next node */
			boost::shared_ptr<Node> next(){
				return m_next;
			}

			/** get age bucket */
			AgeBucket*& bucket(){
				return m_ageBucket;
			}

            /** Removes the object from node, thereby removing it from all indexes and allows it to be RAII-deleted soon */
            void remove()
            {
                if( m_ageBucket != nullptr && this->value() != nullptr ){
    				// run external deletion on node destruction
    				m_mgr->m_owner->deleteItemExt(this->value());
    				// say no external value is managed more by this node
                    this->value(nullptr);

                	long long weight = m_mgr->m_owner->tellWeight(this->value());
                	if ( std::atomic_fetch_sub_explicit (&m_mgr->m_owner->m_currentCapacity, weight, std::memory_order_release) == 1 ) {
                		std::atomic_thread_fence(std::memory_order_acquire);
                	}
                }
            }
		};

        LRUCache<ItemType_>* m_owner;                       /**< Lifespan owner - the Cache */

        const int            _checkOnceInMinutes = 10;      /**< rhythm, in minutes, of validity check routine invocation (not less than) */
        const int            _defaultTimeSliceInHours = 6;  /**< default time slice */

        const int m_numberOfBucketsLimit = 2000;            /**< supported number of buckets */
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
        	if( m_owner->m_totalWeight - m_owner->m_currentCapacity > m_owner->m_capacityLimit ){
        		// go over indexes and note their capacity
        		for(auto it = m_owner->m_indexList->begin(); it != m_owner->m_indexList->end(); ++it){
        			m_owner->m_currentCapacity = it->second->rebuildIndex();
        		}
        		m_owner->m_totalWeight.store(m_owner->m_currentCapacity);

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

        /** lookup for Age Bucket to fit specified timestamp
         * @param timestamp - timestamp to get the Bucket for
         *
         * @return Age Bucket if one found, nullptr otherwise
         */
        AgeBucket* getBucketForTimestamp(boost::posix_time::ptime timestamp){
        	// if there's ancient timestamp specified, reply no bucket exists:
        	if(timestamp < m_startTimestamp)
        		return nullptr;

        	// calculate index in array of buckets that fit requested timestamp:
        	long long idx = timestamp_to_key(timestamp);
            auto it = std::find(m_bucketsKeys->begin(), m_bucketsKeys->end(), idx);
            if(it == m_bucketsKeys->end())
            	return nullptr;

            // key was found
            return (*m_buckets)[(*it)];
        }

        boost::shared_ptr<INode> add(ItemType_* value)
        {
        	// wrap the item to managed Node
        	boost::shared_ptr<Node> sp(new Node(this, value));
        	// and touch it to mark as active and move to corresponding Lifespan Manager Age bucket
            sp->touch();
            return sp;
        }

        /** checks to see if cache is still valid and if LifespanMgr needs to do maintenance */
        void checkValid(){
        	boost::posix_time::ptime now = boost::posix_time::microsec_clock::local_time();

        	// If lock is currently acquired, just skip and let next touch() perform the cleanup.
        	if(now > m_checkTime)
        	{
        		boost::mutex::scoped_lock lock(*lifespan_mux(), boost::try_to_lock);
        		if(lock){
        			if(now > m_checkTime){
        				// if cache is no longer valid throw contents away and start over, else cleanup old items
        				if( m_numberOfBuckets > m_numberOfBucketsLimit || (m_owner->m_isValid != nullptr && !m_owner->m_isValid()) ){
        					// unlock lifespan manager so far and let it run cleanup
        					lock.unlock();
        					m_owner->clear();
        				}
        				else{
        					lock.unlock();
        					cleanUp(now);
        				}
        			}
        		}
        		// check is completed. Update next cache check timestamp:
        		now = boost::posix_time::microsec_clock::local_time();
        		m_checkTime = now + boost::posix_time::minutes(_checkOnceInMinutes);
        	}
        }

        /** Remove old items or items beyond capacity from LifespanMgr.
         *  Note - since we do not physically move items when touched we must check items in bag to determine if they should be deleted.
         *  or moved.  Also Nodes that were removed by setting value to null get removed now.  Removing an item from LifespanMgr allows
         *  them to be cleared from index later.  If removed item is retrieved by index where weak references are stored, it will be re-added to LifespanMgr.
         *
         *  Note: this routine has no internal lock, therefore should be called in the guarded context
         */
        void cleanUp(boost::posix_time::ptime now)
        {
        	// cleanup will be called only if cache capacity overflows
        	int weightToRemove = m_owner->m_currentCapacity - m_owner->m_capacityLimit;

        	boost::mutex::scoped_lock lock(*lifespan_mux());

        	auto it = m_bucketsKeys->begin();

        	// go over buckets, from very old to newer, until the necessary cleanup is done:
        	while( (weightToRemove) > 0 && it != m_bucketsKeys->end()) {
            	// get the key to describe the oldest bucket:
            	long long key = (*(it));

            	// get the oldest bucket:
            	AgeBucket* bucket = (*m_buckets)[key];
                bool deletePermitted = true;

                // go over nodes under this bucket:
        		boost::shared_ptr<Node> node = bucket->first;

        		while(node){
        			// note the node next to current one
        			boost::shared_ptr<Node> next = node->next();

        			if( node->value() != nullptr && node->bucket() != nullptr ){
        				if( node->bucket() == bucket ) {
        					// item has not been touched since bucket was closed, so remove it from LifespanMgr if it is allowed for removal.
                            if(!m_owner->checkRemovalApproval(node->value())){
                            	// no approval for item removal received. Deny the age bucket removal
                            	deletePermitted = false;
                            	// try next node:
                            	node = next;
                            	continue;
                            }
        					// get the weight the item will release:
        					long long toRelease = m_owner->tellWeight(node->value());
        					weightToRemove -= toRelease;

        					if ( std::atomic_fetch_sub_explicit (&m_owner->m_currentCapacity, toRelease, std::memory_order_release) == 0 ) {
        						std::atomic_thread_fence(std::memory_order_acquire);
        						break;
        					}
                			// remove the node
                		    node->remove();
                		    // cut off it from registry
                			node.reset();
        				}
        				else {
        					// item has been touched and should be moved to correct age bag now
        					node->next() = node->bucket()->first;
        					// and point another Age Bucket to this node as to the first node:
        					node->bucket()->first = node;
        				}
        			}
        			node = next;
        		}

        		if(!deletePermitted)
        			continue; // go next bucket if current bucket deletion is denied (as its node is restricted from deletion externally)

        		// drop the bucket from set of buckets:
        		m_buckets->erase(key);
        		// delete the non-needed bucket:
        		delete bucket;
        		// drop the key from key list:
        		m_bucketsKeys->erase(it++);

				if ( std::atomic_fetch_sub_explicit (&m_numberOfBuckets, 1u, std::memory_order_release) == 0u ) {
					std::atomic_thread_fence(std::memory_order_acquire); // all buckets were cleaned up
					break;
				}
        	}
        	lock.unlock();
        	checkIndexValid();
        }

        /** Remove all items from LifespanMgr and reset */
        void clear() {
        	boost::mutex::scoped_lock lock(*lifespan_mux());
        	for(bagsIter it = m_buckets->begin(); it != m_buckets->end(); ++it){
        		boost::shared_ptr<Node> node = (*it).second->first;
        		while(node){
        			boost::shared_ptr<Node> next = node->next();
        			node.reset();
        			node = next;
        		}
        		delete (*it).second;
        	}
        	lock.unlock();
        	// reset item counters
        	m_owner->m_currentCapacity = m_owner->m_totalWeight = 0l;
        	// reset age buckets
        	openBucket(m_startTimestamp);
        }

        /** get ready a new AgeBucket for usage. Close the previous one */
        void openBucket(boost::posix_time::ptime start)
        {
        	boost::mutex* mux = lifespan_mux();
        	boost::mutex::scoped_lock lock(*mux);

        	// close last age bag
        	if( m_currentBucket != nullptr )
        		m_currentBucket->stopTime = start;

        	// open new age bag for next time slice
        	AgeBucket* newBucket = new AgeBucket();

        	// create the key for this bucket
        	long long idx = timestamp_to_key(start);

        	std::pair<long long, AgeBucket*> bucket_pair (idx, newBucket);
        	m_buckets->insert(bucket_pair);
        	m_bucketsKeys->push_back(idx);

        	newBucket->startTime = start;
        	newBucket->first.reset();

            // increment number of buckets:
            std::atomic_fetch_add_explicit (&m_numberOfBuckets, 1u, std::memory_order_relaxed);

        	// say current bucket is a new one:
        	m_currentBucket = newBucket;
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
            boost::shared_ptr<Node> internalCurrent = boost::shared_polymorphic_downcast<Node>(currentNode);
        	if(internalCurrent->next()){ // if there's something next exists
        		if(internalCurrent->next()->value() != nullptr)
        			return internalCurrent->next();
        		else{
        			ret = internalCurrent->next();
        			return getNextNode(idx, ret);
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

	long long                       m_capacityLimit;      /**< cache capacity limit, configurable. We use 90% from configured value */
	mutable std::atomic<long long>  m_currentCapacity;    /**< current cache capacity, in regards to capacity units configured. It is temporary value */
	mutable std::atomic<long long>  m_totalWeight;        /**< total weight of all items hosted currently by cache */

	boost::posix_time::ptime        m_startTime;          /**< cache oldest item timestamp */


	/** external call to get the item weight */
    long long tellWeight(ItemType_* item){
    	return m_tellWeightPredicate(item);
    }

    /** predicate to get the item timestamp */
    boost::posix_time::ptime tellTimestamp(ItemType_* item){
    	return m_tellItemTimestamp(item);
    }

    /** external call to get the item deletion approval */
    bool checkRemovalApproval(ItemType_* item){
    	return m_tellItemIsIdle(item);
    }

    /** run externally defined scenario "on item deleted" */
    void deleteItemExt(ItemType_* item){
    	m_itemDeletionPredicate(item);
    }

    /** Add an item to the cache */
    boost::shared_ptr<INode> addInternal(ItemType_* item) {
    	if( item == nullptr )
    		return nullPtr;

         // see if item is already in index
         boost::shared_ptr<INode> node = nullPtr;
         for(auto idx : (*m_indexList)){
        	 if((node = idx.second->findItem(item)))
        		 break;
         }

         // duplicate is prevented from being added into the cache
         bool duplicate = (node != nullptr && node->value() == item);
         if( duplicate )
        	 return node;

         node = m_lifeSpan->add(item);
         // make sure node gets inserted into all indexes
         for(auto item : (*m_indexList)){
        	 item.second->add(node);
         }

         long long weight = tellWeight(item);
         std::atomic_fetch_add_explicit (&m_totalWeight, weight, std::memory_order_relaxed);
         return node;
     }

	public:

	/** construct the LRU cache object
	 *
	 * @param startFrom - timestamp, create the start time point in order to accept only items with later timestamps into the cache
	 * @param isValid   - predicate used to determine if cache is out of date.  Called before index access
	 *
	 */
    LRUCache(boost::posix_time::ptime startFrom,  long long capacity, isValidPredicate isValid = 0) : m_startTime(startFrom){

    	m_capacityLimit = capacity * 0.9; // get the idea how many capacity Cache is allowed for

        m_isValid  = isValid;
        m_lifeSpan = new LifespanMgr(this, startFrom);

        m_currentCapacity.store(0l);
        m_totalWeight.store(0l);

        m_indexList = new std::unordered_map<std::string, IIndexInternal*>();
    }

    /** cleanup */
    ~LRUCache() {
    	clear();
    	for(auto item : (*m_indexList)){
    		delete item.second;
       	}

    	if(m_lifeSpan != nullptr)
    		delete m_lifeSpan;
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
     * @param indexName - the name to be associated with this list
     * @param getKey    - predicate to get key from object
     * @param loadItem  - predicate to load object if it is not found in index
     *
     * @return newly created index
     * */
    template<typename KeyType_>
    IIndex<KeyType_>* addIndex(std::string indexName, GetKeyFunc<KeyType_> getKey, LoadItemFunc<KeyType_> loadItem)
    {
        Index<KeyType_>* index = new Index<KeyType_>(this, getKey, loadItem);
        (*m_indexList)[indexName] = index;
        return index;
    }

    /** Add an item to the cache (not needed if accessed by index) */
    void add(ItemType_* item)
    {
    	// items that are issued earlier than specified in m_startTime are rejected as well as null-items:
    	if(item == nullptr || (m_tellItemTimestamp && m_tellItemTimestamp(item) < m_startTime))
    		return;

        addInternal(item);
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
};
}

#endif /* LRU_CACHE_HPP_ */
