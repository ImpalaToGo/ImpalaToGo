/*
 * @file  : delimited-text-parser-json.h
 *
 * @brief : Defines JSON implementation of delimited text parser.
 *          utilizes rapidjson for parsing phase itself and handles batch truncation
 *          in applicative way.
 *
 * @date   : Apr 12, 2015
 * @author : elenav
 */

#ifndef DELIMITED_TEXT_PARSER_JSON_H_
#define DELIMITED_TEXT_PARSER_JSON_H_

#include <rapidjson/reader.h>
#include <boost/function.hpp>

#include "exec/delimited-text-parser.h"

namespace impala{

/** JSON parser implementation of delimited text parser */
class JsonDelimitedTextParser : public DelimitedTextParser {

	typedef uint32_t word_t;

	/** represents bitset */
	struct bitset {
		word_t* words;     /**< address the bitset */
		int     nwords;    /**< number of words hosted by bitset */
		int     nbits;     /**< number of bits */
	};

	enum { WORD_SIZE = sizeof(word_t) * 8,
	       MAX_PATH  = 256
	     };

	static inline bool check_bounds(bitset *set, int bit) {
	    if (set->nbits < bit) {
	    	return false;
	    }
	    return true;
	}

	/** Reset bitset (to zeroes) */
	void bitset_clear(bitset* data){
		memset(data->words, 0, WORD_SIZE * data->nwords);
	}

	/** Allocate bitset */
	bitset* bitset_alloc(int num_bits) {
	    bitset *set = (bitset*)(malloc(sizeof(bitset)));
	    set->nwords = (num_bits / WORD_SIZE + 1);
	    set->words = (word_t*)malloc(WORD_SIZE * set->nwords);
	    set->nbits = WORD_SIZE * set->nwords;
	    bitset_clear(set);
	    return set;
	}

	/** release bitset */
	void bitset_free(struct bitset *bitset) {
	    free(bitset->words);
	    free(bitset);
	}

	inline int bindex(int b) { return b / WORD_SIZE; }
	inline int boffset(int b) { return b % WORD_SIZE; }

	bool set_bit(bitset* data, int b) {
		if(!check_bounds(data, b))
			return false;
		data->words[bindex(b)] |= (1 << (boffset(b)));
		return true;
	}

	bool clear_bit(bitset* data, int b) {
		if(!check_bounds(data, b))
			return false;
	    data->words[bindex(b)] &= (~(1 << (boffset(b))));
	    return true;
	}

	int get_bit(bitset* data, int b) {
		if(!check_bounds(data, b))
			return -1;
	    return data->words[bindex(b)] & (1 << (boffset(b)));
	}

private:
	/** functor to handle "simple column detected" event */
	typedef boost::function<void(int len, char** data, int* num_fields,
			FieldLocation* field_locations, PrimitiveType columnType, const std::string& key)> simpleColumnDetected;

	/** functor to handle "object column detected" event */
	typedef boost::function<void(int len, char** next_column_start, int* num_fields,
			FieldLocation* field_locations)> compoundColumnDetected;

    /** predicate to set the array index of the object which became "current" when the array is completed */
	typedef boost::function<void(int idx)> setParentArrayIndex;

	/** predicate to report an empty object on top */
	typedef boost::function<void(int* num_fields, FieldLocation* field_locations)> reportEmptyObject;

    struct JSONObjectType{
		enum object_type{
			ENTITY,
			ARRAY,
			FIELD  /** not used, just for complete types representation */
		};
	};

	/** Defines handler for rapidjson reader callbacks.
	 * Thread-safe
	 */
	struct JsonSAXParserEventsHandler : public rapidjson::BaseReaderHandler<rapidjson::UTF8<>, JsonSAXParserEventsHandler > {
		/** represents JSON record node (object) along with its state during SAX parsing */
		struct JsonObject{
			JSONObjectType::object_type type;      /**< object type */
			JsonObject*                 parent;    /**< object parent. For JSON root, its NULL */
			JsonObject*                 array;     /**< enclosing array if any */
			char*                       key;       /**< the key for this object. For root JSON, its empty */
			int                         keylen;    /**< legth of the key */
			bool                        completed; /**< flag, indicates that this object was completely done */
			int                         index;     /**< index is set if the object is a child of some parent array.
			                                        * Example : {"arr" : [{"a" : "val", "b" : "val"}, {"b" : "val"}, {"a" : "val"}]}
			                                        * Index = -1 means no parent array exists for this object.
			                                        */
			int                         children;  /**< holds number of direct children of this object.
			                                        * For object this would be the number of fields of level 0,
			                                        * For array, number of elements in array
			                                        */

			JsonObject() : type(JSONObjectType::ENTITY), parent(NULL), array(NULL), key(NULL), keylen(0), completed(false),
					index(-1), children(0) {}

			~JsonObject() {
				if(key != NULL) delete [] key;
				key = NULL;
			}
		};

	        /** iterator for json objects registry */
		typedef std::vector<JsonObject*>::iterator jsonObjectsIt;

        JsonSAXParserEventsHandler():
        	m_currentKeylen(0),
			m_materializedFields(NULL),
			m_fieldLocations(NULL),
			m_columnCallback(0),
			m_reportEmptyObject(0),
			m_compundColumnCallback(0),
			m_incompleteObjects(0),
        	m_isConfigured(false),
        	m_isReady(false),
			m_isFresh(true),
			m_currentObject(NULL),
			m_currentArray(NULL),
			state_(kExpectObjectStart)
		{
        	memset(&m_currentKey, 0, MAX_PATH);
		}

        /** Ctor.
         * @param callbackOnColumnFound - predicate to be fired when simple column is found.
         * Currently is the only used predicate
         *
         * @param callbackCompundColumnFound - predicate to be fired when nested entity is found
         */
        JsonSAXParserEventsHandler(simpleColumnDetected callbackOnColumnFound,
        		setParentArrayIndex callbackParentArrayIndexSetter,
				reportEmptyObject callbackReportEmptyObject,
				compoundColumnDetected callbackCompundColumnFound) :
					  m_currentKeylen(0),
					  m_materializedFields(NULL),
					  m_fieldLocations(NULL),
					  m_columnCallback(callbackOnColumnFound),
					  m_setCurrentObjectArrayIndex(callbackParentArrayIndexSetter),
					  m_reportEmptyObject(callbackReportEmptyObject),
					  m_compundColumnCallback(callbackCompundColumnFound),
					  m_incompleteObjects(0),
					  m_isConfigured(false),
					  m_isReady(true),
					  m_isFresh(true),
					  m_currentObject(NULL),
					  m_currentArray(NULL),
					  state_(kExpectObjectStart)
        {
        	memset(&m_currentKey, 0, MAX_PATH);
        }

        /** configure the message handler with new registry of fields
         * @param fieldLocations - plain registry of field locations, to be filled in
         * @param pfields_num    - reference to
         */
        inline void configure(FieldLocation* fieldLocations, int* pfields_num){
        	m_fieldLocations = fieldLocations;
        	m_materializedFields = pfields_num;
        	m_isConfigured = true;
        	m_isReady = m_isFresh;
        }

        /** reset the completed event handler before to start new session or just state reset
         *  during session continuation.
         *
         *  @param continuation - flag, indicates whether JSON record continuation is requested
         *  @param new_session  - flag, indicates whether completely new session is required on
         *  current handler
         */
        inline void reset(bool continuation = false, bool new_session = false){

        	// reset the state
        	state_ = kExpectObjectStart;

        	// don't reset the rest of state if continuation is requested
        	if(continuation)
        		return;

        	boost::mutex::scoped_lock lock(m_incompleteObjectsMux);

        	// reset number of incomplete objects registered:
        	m_incompleteObjects = 0;

        	// cleanup the vector of registered JSON objects:
        	cleanvector(m_objects);
        	// no "currents" exists:
        	m_currentObject = NULL;
        	m_currentArray = NULL;

        	memset(&m_currentKey, 0, MAX_PATH);
        	m_currentKeylen = 0;

        	if(new_session)
        		// no fields were materialized so far:
        		m_materializedFields = NULL;


        	// say the handler is fresh now
            m_isFresh = true;
        	m_isConfigured = false;
        }

        /** getter to find whether the handler is ready to handle new session.
         *  Handler is ready in case if it is configured and doesn't have incompleted
         *  objects from its previous session
         */
        inline bool ready(){
        	return m_isReady = (m_isReady && (number_of_incomplete_objects(true) == 0));
        }

        template <class T>
        void cleanvector(std::vector<T*>& v) {
            while(!v.empty()) {
                delete v.back();
                v.pop_back();
            }
        }

        ~JsonSAXParserEventsHandler() {
        	// cleanup the registry of JSON objects:
        	cleanvector(m_objects);
        }

        /******************************* BaseReaderHandler implementation *****************************************************/
        /**********************************************************************************************************************/

	    bool Null(const Ch* data, rapidjson::SizeType len) {
	    	m_columnCallback(len, const_cast<char**>(&data), m_materializedFields, m_fieldLocations,
	    			TYPE_NULL, build_fqp());
	    	state_ = kExpectNameOrObjectEnd;
	    	return true;
	    }

	    bool Bool(bool b, const Ch* data, rapidjson::SizeType len) {
	    	m_columnCallback(len, const_cast<char**>(&data), m_materializedFields, m_fieldLocations,
	    			TYPE_BOOLEAN, build_fqp());
	    	state_ = kExpectNameOrObjectEnd;
	    	return true;
	    }
	    bool Int(int i, const Ch* data, rapidjson::SizeType len) {
	    	m_columnCallback(len, const_cast<char**>(&data), m_materializedFields, m_fieldLocations,
	    			TYPE_INT, build_fqp());
	    	state_ = kExpectNameOrObjectEnd;
	    	return true;
	    }
	    bool Uint(unsigned u, const Ch* data, rapidjson::SizeType len) {
	    	m_columnCallback(len, const_cast<char**>(&data), m_materializedFields, m_fieldLocations,
	    			TYPE_INT, build_fqp());
	    	state_ = kExpectNameOrObjectEnd;
	    	return true;
	    }
	    bool Int64(int64_t i, const Ch* data, rapidjson::SizeType len) {
	    	m_columnCallback(len, const_cast<char**>(&data), m_materializedFields, m_fieldLocations,
	    			TYPE_BIGINT, build_fqp());
	    	state_ = kExpectNameOrObjectEnd;
	    	return true;
	    }
	    bool Uint64(uint64_t u, const Ch* data, rapidjson::SizeType len) {
	    	m_columnCallback(len, const_cast<char**>(&data),m_materializedFields, m_fieldLocations,
	    			TYPE_BIGINT, build_fqp());
	    	state_ = kExpectNameOrObjectEnd;
	    	return true;
	    }
	    bool Double(double d, const Ch* data, rapidjson::SizeType len) {
	    	m_columnCallback(len, const_cast<char**>(&data), m_materializedFields, m_fieldLocations,
	    			TYPE_DOUBLE, build_fqp());
	    	state_ = kExpectNameOrObjectEnd;
	    	return true;
	    }
	    bool String(const char* data, rapidjson::SizeType len, bool copy) {
	    	m_columnCallback(len, const_cast<char**>(&data), m_materializedFields, m_fieldLocations,
	    			TYPE_STRING, build_fqp());
	    	state_ = kExpectNameOrObjectEnd;
	        return true;
	    }
	    bool StartObject() {
	    	switch (state_) {
	    	case kExpectObjectStart:
	    	case kExpectValue:
	    	{
	    		state_ = kExpectNameOrObjectEnd;
	    		// set the "current" object (NULL for JSON root or previous found nested object otherwise)
	    		// as the parent of new object. Mark is as "incomplete"
	    		JsonObject* object = new JsonObject();
	    		object->type = JSONObjectType::ENTITY;
	    		object->parent = m_currentObject;
                object->array = m_currentArray;

	    		if(m_currentKeylen){
	    			object->key = new char[m_currentKeylen];
	    			memset(object->key, 0, m_currentKeylen);
	    			memcpy(object->key, m_currentKey, m_currentKeylen);
	    			object->keylen = m_currentKeylen;
	    		}
	    		object->completed = false;
	    		{
	    			boost::mutex::scoped_lock lock(m_incompleteObjectsMux);
	    			// if current object is the array (so array is the nearest parent for this object),
	    			// assign the corresponding index to this object to track it as its enclosing array child
	    			if(m_currentObject != NULL){
	    				if(m_currentObject->type == JSONObjectType::ARRAY)
	    					object->index = m_currentObject->children;
	    				// if there was some index assigned to any of parents of this object that were a
	    				// direct child of array, derive the index from them
	    				else {
	    					object->index = m_currentObject->index;
	    				}
	    			}
	    			// reassign current object to say that we start to work with this new object
	    			m_currentObject = object;

	    			// put new object into registry of objects
	    			m_objects.push_back(object);
	    		}
	    	}
	    		return true;
	    	default:
	    		return false;
	    	}
	    }
	    bool Key(const char* str, rapidjson::SizeType length, bool copy) {
	    	memset(&m_currentKey, 0, MAX_PATH);
	    	memcpy(&m_currentKey, str, length);
	    	m_currentKeylen = length;

	    	state_ = kExpectValue;
	        return true;
	    }
	    bool EndObject(rapidjson::SizeType memberCount) {
	    	bool a_part_of_array = m_currentObject->array != NULL;
	    	bool ret = state_ == kExpectNameOrObjectEnd;
	    	if(!ret){
	    		std::cout << "Unexpected state!" << std::endl;
	    	}
	    	if(a_part_of_array){
	    		state_ = kExpectObjectStart;
		    	// handle empty object in the way of signaling this on top to a client
		    	if(!memberCount)
		    		m_reportEmptyObject(m_materializedFields, m_fieldLocations);
	    	}
	    	{
	    		boost::mutex::scoped_lock lock(m_incompleteObjectsMux);
	    		// mark the current object as completed:
	    		m_currentObject->completed = true;

		    	// reassign "current object" to a parent of this object:
		    	m_currentObject = m_currentObject->parent;
		    	if(m_currentObject != NULL){
		    		memset(&m_currentKey, 0, MAX_PATH);
		    		memcpy(&m_currentKey, m_currentObject->key, m_currentObject->keylen);
		    		m_currentKeylen = m_currentObject->keylen;

		    		// if current object is the array (so array is the nearest parent for this object),
	    			// report number of children is increased for array object. We track only direct children for each array
	    			if(m_currentObject->type == JSONObjectType::ARRAY){
	    				++(m_currentObject->children);
	    			}
		    	}
	    	}

	    	return ret;
	    }
	    bool StartArray() {
    		JsonObject* object = new JsonObject();
    		object->type = JSONObjectType::ARRAY;
    		object->parent = m_currentObject;

            // assign enclosing array (if any)
    		object->array = m_currentArray;

    		if(m_currentKeylen){
    			object->key = new char[m_currentKeylen];
    			memset(object->key, 0, m_currentKeylen);
    			memcpy(object->key, m_currentKey, m_currentKeylen);
    			object->keylen = m_currentKeylen;
    		}

    		object->completed = false;
    		{
    			boost::mutex::scoped_lock lock(m_incompleteObjectsMux);

    			// assign array object as the current object:
    			m_currentObject = object;
    			// assign array object as the current array:
    			m_currentArray = object;

    			// put new object into registry of objects
    			m_objects.push_back(object);
    		}
    		state_ = kExpectObjectStart;
	    	return true;
	    }
	    bool EndArray(rapidjson::SizeType elementCount) {
	    	{
	    		boost::mutex::scoped_lock lock(m_incompleteObjectsMux);
	    		// mark the current object as completed:
	    		m_currentObject->completed = true;

		    	// reassign "current object" to a parent of this object:
		    	m_currentObject = m_currentObject->parent;
		    	int array_index;
		    	if(m_currentObject != NULL){
		    		memset(m_currentKey, 0, MAX_PATH);
		    		memcpy(m_currentKey, m_currentObject->key, m_currentObject->keylen);
		    		m_currentKeylen = m_currentObject->keylen;
		    		array_index = m_currentObject->index;
		    	}
		    	else
		    		array_index = -1;
		    	m_setCurrentObjectArrayIndex(array_index);
		    	// if m_currentArray had no enclosing parent array, reset it.
		    	// Set current array to the parent enclosing array if one exists.
		    	m_currentArray = m_currentArray->array;
	    	}
	    	state_ = kExpectNameOrObjectEnd;
	    	return true;
	    }

	    /** check whether the whole json record is processed */
	    bool isValueIncomplete(){ return state_ == kExpectValue; }

	    /** reply with number of processed (materialized) fields. */
	    size_t materialized_fields_size() { return *m_materializedFields; }

	    /** getter for number of incomplete JSON objects registered during last parse session
	     *  @param force_update - flag, indicates whether force recalculation is required
	     *  @return number of non-closed JSON objects in current JSON session
	     */
	    int number_of_incomplete_objects(bool force_update = true) {
	    	boost::mutex::scoped_lock lock(m_incompleteObjectsMux);
	    	if(!force_update)
	    		return m_incompleteObjects;

	    	// reset current number
	    	m_incompleteObjects = 0;
	    	for(jsonObjectsIt it = m_objects.begin(); it != m_objects.end(); it++){
	    		if(!(*it)->completed)
	    			m_incompleteObjects++;
	    	}
	    	return m_incompleteObjects;
	    }

	    /**
	     * reconstructs the non-completed hierarchy of JSON's value (non-string or array) which
	     * failed to be extracted from JSON for last time.
	     * String values and keys are handled properly using regular rapidjson "parse value error"
	     * callbacks.
	     *
	     * @return string representation of prefix to be added to new data
	     * in order to get valid JSON
	     */
	    std::string reconstruct_the_hierarchy(){
	    	std::string hierarchy;

	    	JsonObject* leaf = NULL;
	    	// traverse the vector of JSON objects and reconstruct:
	    	for(jsonObjectsIt it = m_objects.begin(); it != m_objects.end(); it++){
	    		leaf = (*it);

	    		if((*it)->parent == NULL){
	    			hierarchy += "{";
	    			continue;
	    		}

	    		if(!(*it)->completed){
	    			if((*it)->parent->type == JSONObjectType::ENTITY){
	    				hierarchy += "\"";
	    				hierarchy.append((*it)->key, (*it)->keylen);
	    				hierarchy += "\":";
	    			}
	    			hierarchy += (*it)->type == JSONObjectType::ENTITY ? "{" : "[";
	    		}
	    	}

	    	// if the "key" is the node which was parsed last from JSON,
	    	// append it at the end of hierarchy:
            if( (state_ == kExpectValue) && (leaf != NULL) && (m_currentObject->type != JSONObjectType::ARRAY)){
            	hierarchy += "\"";
            	hierarchy.append(m_currentKey, m_currentKeylen);
            	hierarchy += "\":";
            }

	    	return hierarchy;
	    }

	    /** Builds the field's path hierarchy in terms of fully quelified field path, parts
	     * separated with a dot. */
	    std::string build_fqp(){
	    	std::string fqp = "";

	    	for(jsonObjectsIt it = m_objects.begin(); it != m_objects.end(); it++){
	    		// no key for root
	    		if((*it)->parent == NULL)
	    			continue;

                // for all non-completed entities
	    		if(!(*it)->completed && ( (*it)->type == JSONObjectType::ENTITY) ){
	    			// if there was a content in fqp already, put the path's parts separator (which is dot)
	    			if(!fqp.empty())
	    				fqp += ".";
	    			fqp.append((*it)->key, (*it)->keylen);
	    		}
	    	}
	    	// and attach the current key value if any:
            if( (state_ == kExpectValue) && (m_currentObject->type != JSONObjectType::ARRAY)){
            	if(!fqp.empty() && m_currentKeylen)
            		fqp += ".";
            	fqp.append(m_currentKey, m_currentKeylen);
            }
	    	return fqp;
	    }

	    /** getter for current object */
	    JsonObject* currentObject() { return m_currentObject; }

	    /** getter for current array */
	    JsonObject* currentArray()  { return m_currentArray; }

	private:
	    char                   m_currentKey[MAX_PATH];  /**< key that was successfully extracted last. */
	    int                    m_currentKeylen;         /**< current key length */
	    int*                   m_materializedFields;    /**< number of fields materialized during current parser session */
	    FieldLocation*         m_fieldLocations;        /**< externally injected registry of field locations, to be filled in */



	    simpleColumnDetected   m_columnCallback;             /**< callback to be invoked when the simple field is completely extracted */
	    setParentArrayIndex    m_setCurrentObjectArrayIndex; /**< callback to set the array index of the object which became
	                                                           * "current" when the array is completed */

	    reportEmptyObject      m_reportEmptyObject;          /**< callback to report an empty object to a client */

	    compoundColumnDetected m_compundColumnCallback; /**< callback to be invoked when the entity is found */

	    /** Lock for incomplete objects access */
	    boost::mutex m_incompleteObjectsMux;

	    /** number of incomplete objects currently registered by handler */
	    int m_incompleteObjects;

		/************************ Readiness section ****************************************/
		/** flag is set when the handler is configured with sink (FieldLocations) */
		bool m_isConfigured;

		/** flag is set when the parser is ready for new session */
		bool m_isReady;

		/** Flag is set when the state was explicitly reset from outer scope */
		bool m_isFresh;

        std::vector<JsonObject*> m_objects;         /**< registry of JSON ibjects found during SAX parsing session. Root, entities, arrays */
        JsonObject*              m_currentObject;   /**< currently handled entity or array */

        JsonObject*              m_currentArray;    /**< here always non-closed current array is sitting. */

        /** States for parser error handling state machine */
	    enum State {
	    	kExpectObjectStart,       /**< we expect next the object to start, "{" */
			kExpectNameOrObjectEnd,   /**< we expect next the key or "}" */
			kExpectValue,             /**< we expect value next */
	    } state_;
	};

	struct SchemaMapping {
		int column_idx;
		int llvm_tuple_idx;

		SchemaMapping():column_idx(-1), llvm_tuple_idx(-1) {}
		SchemaMapping(int col_idx, int tuple_idx):column_idx(col_idx), llvm_tuple_idx(tuple_idx){}

		bool defined() const { return column_idx != -1 && llvm_tuple_idx != -1;}
	};
 
 public:
	virtual ~JsonDelimitedTextParser();

	/**
	 * JSON parser, currently in use in context of plain JSON parsing while
	 * can be extended for schema discovery and hierarchical data parsing.
	 *
	 * num_cols is the total number of columns including partition keys.
	 *
	 * is_materialized_col should be initialized to an array of length 'num_cols', with
	 * is_materialized_col[i] = <true if column i should be materialized, false otherwise>
	 * Owned by caller.
	 * The main method is ParseFieldLocations which fills in a vector of pointers and lengths to the fields.
	 */
	JsonDelimitedTextParser(int num_cols,
			int num_partition_keys,
			const bool* is_materialized_col,
			char tuple_delim);

	/**
	 *   Parses a byte buffer for the field and tuple breaks.
	 *   This function will write the field start & len to field_locations
	 *   which can then be written out to tuples.
	 *
	 *   Input Parameters:
	 *   max_tuples          : The maximum number of tuples that should be parsed.
	 *                         This is used to control how the batching works.
	 *   remaining_len       : Length of data remaining in the byte_buffer_pointer.
	 *   byte_buffer_pointer : Pointer to the buffer containing the data to be parsed.
	 *
	 *   Output Parameters:
	 *   field_locations   :  array of pointers to data fields and their lengths
	 *   num_tuples        :  Number of tuples parsed
	 *   num_fields        :  Number of materialized fields parsed
	 *   next_column_start :  pointer within file_buffer_ where the next field starts
	 *                        after the return from the call to ParseData
	 *
	 *   Note, for JSON parser this method may internally allocate data (using memory pool).
	 *   Therefore, after the batch is processed, and all data is consumed and copied into
	 *   sink, parser should be reset.
	 */
	Status ParseFieldLocations(int max_tuples,
			int64_t remaining_len,
			char** byte_buffer_ptr,
			char** row_end_locations,
			FieldLocation* field_locations,
			int* num_tuples,
			int* num_fields,
			char** next_column_start);


	/** Configure JSON paths to schema mapping */
	void setupSchemaMapping(const std::vector<SlotDescriptor*>& schema);

 private:

	/** predicate to handle column detection */
	simpleColumnDetected   m_columnDetectedHandler;

	/** predicate to keep the parser up to date with the array index of current object within the underlying parser */
	setParentArrayIndex    m_setCurrentArrayIndex;

	/** predicate to handle empty object, a part of mapped array, in continuation (reconstruction) scenario */
	reportEmptyObject      m_reportEmptyObject;

	/** predicate to handle compound column detection */
	compoundColumnDetected m_compundColumnDetectedhandler;

	/** schema mapping, key is the column fully qualified path,
	 * value is the set of column position in the original schema and the position generated by llvm for the slot */
	boost::unordered_map<std::string, SchemaMapping> m_schema;

	/** flag, indicates the fact that the schema is defined */
	bool m_schema_defined;

	/** one-time calculated configured schema size */
	int schema_size;

	/** size of data remainder if any which comes with a current batch */
	int m_data_remainder_size;

	/** points to next tuple start */
	int32_t m_next_tuple_start;

	/********************* file-span boundaries handling section ******************/

	/**
	 * buffer that contains last unfinished tuple part reconstructed according to its hierarchy in JSON
	 * + the part from new batch until enclosing "}", e.g., the data which is enough to complete the parsing
	 * of the tuple that was started within previous session. This buffer may be only one per batch
	 * and it should be hold by parser until the parser client will call reset after the parsing is finished
	 * which should mean that the client consumed data from this cache (copied it)
	 */
	char* m_reconstructedRecordData;

	/** buffer to store data that was no processed on previous parse session due
     *  JSON record was truncated. This buffer is temporary for parser and always holds the content which the last
     *  parsing session failed to parse
	 */
	char* m_unfinishedRecordData;

	/** length of content of last incompleted record */
	int m_unfinishedRecordLen;

	/** schema mapping of currently found column (if any) */
	SchemaMapping m_mapping;

	/** Tuple slots parse status bitmap. Each bit represents the slot parse event.
	 * E.g., bit is set if slot was parsed.
	 */
	bitset* m_tuple;

	/** number of fields materialized for current tuple. Should be reset per tuple */
	int m_number_of_materialized_fields;

	/** flag that signals that the continuation of the previous batch is requested.
	 * This flag is only for "add column" scenario and should not be mixed with other
	 * continuation (like used in ParseFieldLocations() stack) control flags */
	bool m_continiation_flag;

	/** tracks the current record (tuple) index in the JSON's collection to distinguish records mapped
	 * to JSON's collection elements */
	int m_record_idx_in_json_collection;

	/** Number of tuples found in current batch */
	int m_num_tuples;

	/*********************** rapdijson callbacks handling section  ******************/

	/** we preserve message handler */
	boost::scoped_ptr<JsonSAXParserEventsHandler> m_messageHandler;

	/** parse single tuple */
	void parseSingleTupleInternal(int64_t len, char* buffer, FieldLocation* field_locations,
			int* num_fields, const bool flag);

	/** initialize parser-specific search characters registry */
	void setupSearchCharacters();

	/** method makes the decision whether current column should be materialized */
	bool virtual ReturnCurrentColumn() const;

	void addColumnInternal(int len, char** data, int* num_fields,
			FieldLocation* field_locations, PrimitiveType type = INVALID_TYPE,
			const std::string& key = "", const bool flag = false);

	/** report new tuple found */
	void reportNewTuple();

	/** reset the parser according to parser implementation specifics */
	void parserResetInternal(bool hard = true);

	/** State handler, detects whether previous batch parsing session should be continued
	 *  (considering previously handled JSON record was truncated)
	 *
	 *  @return flag, indicates whether previous session continuation is required.
	 *  True if so.
	 */
	bool continuePreviousSession(char** data, int64_t* len);

	/* Updates the record index within the JSON collection (if any) which is encountered for currently handled entity
	 * by underlying parser
	 *
	 * @param index - index to assign
	 * */
	inline void updateCurrentArrayIndex(int index) { m_record_idx_in_json_collection = index; }

	/** Handles "empty object" event arrived from underlying JSON parser session */
	inline void handleEmptyObject(int* num_fields, FieldLocation* field_locations){
		// Handles special case when :
		// 1. we have a mapping to a field of object inside collection
		// 2. previous batch was truncated in the way that :
		// - collection parsing was not accomplished (no ] appears)
		// - schema size > 1
		// - at least one of fields was materialized from the object inside collection, following condition is satisfied:
		// 1 > num_materialized_tuples < schema_size
		// - the object the materialized field belongs to was not accomplished (no } appeared)
		// there's only 1 element in the collection to parse

		// Example:
		// previous batch :
		// ... "array":[{"mapped_field_1":"value"
		// current batch:
		// {], .....
		// reconstructed part:
		// .. "array":[{}]
		if(m_schema_defined && m_continiation_flag && m_number_of_materialized_fields){
			// fill remained columns for this tuple
			FillColumns<false>(0, NULL, num_fields, field_locations);
			reportNewTuple();
		}
	}
};

}

#endif /* DELIMITED_TEXT_PARSER_JSON_H_ */
