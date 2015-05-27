/*
 * @file  : delimited-text-parser-json.cc
 *
 * @brief : Contains JSON implementation of delimited text parser.
 *          utilizes rapidjson for parsing phase itself and handles batch truncation
 *          in applicative way.
 *
 * @date   : Apr 12, 2015
 * @author : elenav
 */

#include <iostream>

#include <boost/bind.hpp>
#include <boost/mem_fn.hpp>

#include "rapidjson/memorystream.h"
#include "rapidjson/error/error.h"

#include "exec/delimited-text-parser.inline.h"
#include "exec/delimited-text-parser-json.inline.h"

namespace impala{

JsonDelimitedTextParser::JsonDelimitedTextParser(int num_cols,
		int num_partition_keys,
		const bool* is_materialized_col,
		char tuple_delim) :
		DelimitedTextParser(num_cols, num_partition_keys, is_materialized_col, tuple_delim),
		m_compundColumnDetectedhandler(0),
		m_schema_defined(false),
		schema_size(0),
		m_data_remainder_size(-1),
		m_next_tuple_start(-1),
		m_reconstructedRecordData(NULL),
		m_unfinishedRecordData(NULL),
		m_unfinishedRecordLen(-1),
		m_tuple(NULL),
		m_number_of_materialized_fields(0),
		m_record_idx_in_json_collection(-1),
		m_num_tuples(0){

	// bind "column detected" handler to this parser to be handled here
	m_columnDetectedHandler = boost::bind(boost::mem_fn(&DelimitedTextParser::AddColumn<false>), this,
			_1, _2, _3, _4, _5, _6);

	m_setCurrentArrayIndex = boost::bind(boost::mem_fn(&JsonDelimitedTextParser::updateCurrentArrayIndex), this, _1);
	m_reportEmptyObject = boost::bind(boost::mem_fn(&JsonDelimitedTextParser::handleEmptyObject), this, _1, _2);

	// create the rapidjson events handler:
	m_messageHandler.reset(new JsonSAXParserEventsHandler(m_columnDetectedHandler,
			m_setCurrentArrayIndex,
			m_reportEmptyObject,
m_compundColumnDetectedhandler));

	// configure search characters registry
	setupSearchCharacters();

	// reset the parser
	parserResetInternal();
}

JsonDelimitedTextParser::~JsonDelimitedTextParser(){
	//if(m_tuple != NULL)
	//	bitset_free(m_tuple);
	//m_tuple = NULL;
}

void JsonDelimitedTextParser::setupSearchCharacters(){
	// Initialize the sse search registers.
    char search_chars[SSEUtil::CHARS_PER_128_BIT_REGISTER];
    memset(search_chars, 0, sizeof(search_chars));

	if (tuple_delim_ != '\0') {
		search_chars[num_delims_++] = tuple_delim_;
		// Hive will treats \r (^M) as an alternate tuple delimiter, but \r\n is a
		// single tuple delimiter.
		if (tuple_delim_ == '\n') search_chars[num_delims_++] = '\r';
		xmm_tuple_search_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(search_chars));
	}
}

void JsonDelimitedTextParser::parserResetInternal(bool hard){
	m_data_remainder_size = -1;

	m_num_tuples = 0;

	// if there were buffer created for data reconstruction, release it:
	if(m_reconstructedRecordData != NULL){
		delete [] m_reconstructedRecordData;
		m_reconstructedRecordData = NULL;
	}
	if(hard){
		column_idx_ = num_partition_keys_;
		// reset the counters related to "tuple" tracking mechanism
		m_number_of_materialized_fields = 0;
		m_record_idx_in_json_collection = -1;
	}
}

bool JsonDelimitedTextParser::continuePreviousSession(char** data, int64_t* len){
	bool readiness = m_messageHandler->ready();
    // if the handler is not ready, this means that previous record parsing was
	// not completed due record truncation (or errors).
	// Handler holds the previous state in order to proceed with incomplete tuple in current session.
	if(readiness){
		// if handler is ready, just do nothing.
		return false;
	}

	// if in continuation, set the record index to 0
	m_record_idx_in_json_collection = -1;

	// Get the offset of next record within this batch
	m_next_tuple_start = FindFirstInstance(*data, *len);
	bool more_records_exist = m_next_tuple_start != -1;
	if(more_records_exist){
		m_next_tuple_start -= 1;
	}

    std::string reconstructedPrefix = m_messageHandler->reconstruct_the_hierarchy();
	// analyze the arrived data buffer, we should trim the "," if it exists.
	int pos = 0;
	bool leading_keyvalue_or_field_separator_found = false;
	while(pos < *len){
		if(*((*data) + pos) != ' '){
			if((!m_messageHandler->isValueIncomplete() && *((*data) + pos) == ',') || *((*data) + pos) == ':'){
				leading_keyvalue_or_field_separator_found = true;
				break;
			}
			break;
		}
		else pos++;
	}
    // reset hard the event handler:
    m_messageHandler->reset(false, true);

    // save the size of newly arrived data which we append to the partial record
	// we hold from previous session:
	m_data_remainder_size = more_records_exist ? m_next_tuple_start : *len;
	// if there was a "," at the beginning of current line, skip it
    m_data_remainder_size = leading_keyvalue_or_field_separator_found ? (m_data_remainder_size - (pos + 1) ) : m_data_remainder_size;

	// consider remainder of previous non-finished JSON record inside current batch:
	int64_t new_len = m_unfinishedRecordLen + reconstructedPrefix.length() + m_data_remainder_size + 1;

	// allocate buffer enough to hold "hierarchy of_enclosing_entities" + unfinished JSON record part.
	// we will do the reconstruction of initial data hierarchy in this buffer
	// to present the data as valid JSON
	m_reconstructedRecordData = new char[new_len];
    memset(m_reconstructedRecordData, 0, new_len);

	// and save non-finished content prepended with JSON hierarchy:
	memcpy(m_reconstructedRecordData, reconstructedPrefix.data(), reconstructedPrefix.length());

	// if there was some partial content in previous record
	// which we should re-parse, copy the previous part of JSON record
	// which was not parsed during previous batch session:
	if(m_unfinishedRecordLen != 0)
		memcpy((m_reconstructedRecordData + reconstructedPrefix.length()), m_unfinishedRecordData, m_unfinishedRecordLen);
	// copy from newly arrived byte buffer the remainder of JSON:
	memcpy((m_reconstructedRecordData + reconstructedPrefix.length() + m_unfinishedRecordLen),
			leading_keyvalue_or_field_separator_found ? (*data + pos + 1) : (*data), m_data_remainder_size);
    // reset the incompleted record data buffer only if one was allocated
    if(m_unfinishedRecordData != NULL){
    	// deallocate old remainder:
    	delete [] m_unfinishedRecordData;
    	m_unfinishedRecordData = NULL;
    }
    // if no more records exists in the "data" buffer, increase remained length in order to contain extra
    // content we will add at the beginning of data in order to reconstruct valid JSON:
    if(!more_records_exist){
    	*len += m_unfinishedRecordLen + reconstructedPrefix.length();
    	// we should repoint the data as well:
    	*data = m_reconstructedRecordData;
    	// and reassign data reminder size
    	m_data_remainder_size = *len;
    }

	// save new buffer length:
    m_unfinishedRecordLen = new_len;

	return true;
}

/** Parsing plain JSON data within the batch pointed by "byte_buffer_ptr" into FieldLocation descriptors. */
Status JsonDelimitedTextParser::ParseFieldLocations(int max_tuples, int64_t remaining_len,
    char** byte_buffer_ptr, char** row_end_locations,
	FieldLocation* field_locations,
    int* num_tuples, int* num_fields, char** next_row_start) {

	m_next_tuple_start = -1;

	if (*num_tuples == max_tuples)
		return Status::OK;

	// create the json reader which will handle tuples
	rapidjson::Reader reader;

	// check we need to continue previous session and make cached data aggregation if so
	bool continue_previous_session = m_continiation_flag = continuePreviousSession(byte_buffer_ptr, &remaining_len);

	if(continue_previous_session)
		m_messageHandler->reset(continue_previous_session, true);
	m_messageHandler->configure(field_locations, num_fields);

	// we always go and try to parse what we have in current batch.
	// we say we have new tuple completed when parser gives no error
	// so we will not miss any tuples
	bool initial_tuple_found_flag = true;

	// Handle batch data:
    while (remaining_len > 0) {
		bool new_record = initial_tuple_found_flag;

		// and reset the initial "tuple found" flag:
		initial_tuple_found_flag = false;
		size_t offset = 0;

		if((tuple_delim_ == '\n' && **byte_buffer_ptr == '\r') || (!continue_previous_session && **byte_buffer_ptr == ' ')){
			// we found '\r', go next cycle
			++*byte_buffer_ptr;
			remaining_len--;
			continue;
		}

		if (tuple_delim_ != '\0' && (**byte_buffer_ptr == tuple_delim_)) {
			// go to next tuple start
			++*byte_buffer_ptr;
			remaining_len--;
			// if this is the end of current batch, stop processing
			if(remaining_len == 0)
				break;

			// if we still have bytes to process, set next tuple start to found position:
			m_next_tuple_start = remaining_len;
			// say we found new tuple
			new_record = true;
		}

		*next_row_start = *byte_buffer_ptr;

		// start to handle new tuple
		if (new_record) {
			MemoryStream* ss = NULL;
			// if there's previous parsing session continuation is required,
			// create the stream from reconstructed buffer of unfinished record data and give it to a reader
			// to complete the record we started at the previous session:
			if(continue_previous_session){
				m_messageHandler->reset(continue_previous_session, false);
				ss = new MemoryStream(m_reconstructedRecordData, m_unfinishedRecordLen);
			}
			else {
				m_messageHandler->reset(continue_previous_session, false);
				ss = new MemoryStream(*next_row_start, remaining_len);
				m_next_tuple_start = -1;
			}

			reader.ParseEx<32>(*ss, *(m_messageHandler.get()));

			int  error_offset = -1;
            bool error = false;

            if(reader.HasParseError()){
            	// we ignore the error which is rise by parser in case if it detects
            	// the extra content after root object is closed.
            	// for us it is ok as we may have a lot of records within the single batch
				if(reader.GetParseErrorCode() == rapidjson::kParseErrorDocumentRootNotSingular){
					error = false;
				}
				else error = true;
            }
			if(error){
				error_offset = reader.GetErrorOffset();
				// calculate unfinished content size
				m_unfinishedRecordLen = remaining_len - error_offset;
				// TODO : use MemoryPool allocator instead:
				if(m_unfinishedRecordLen != 0){
					m_unfinishedRecordData = new char[m_unfinishedRecordLen];
					memset(m_unfinishedRecordData, 0, m_unfinishedRecordLen);
					// and save non-finished content for next usage
					memcpy(m_unfinishedRecordData, *next_row_start + error_offset, m_unfinishedRecordLen);
				}
				else {
					if(m_unfinishedRecordData != NULL)
						delete [] m_unfinishedRecordData;
					// reset the data pointer
					m_unfinishedRecordData = NULL;
				}
			}

			// renew offset within current data buffer.
			// is there were failure during parse, we have incompleted record.
			// we are not going to deal with it until next batch, so just move to the data end.
			// Otherwise, go to the next tuple, if any. If no next tuple exists in current batch, just stand for
			// stream last position
			int stream_offset;
			if(continue_previous_session){
				stream_offset = m_data_remainder_size;
				// reset continuation flag:
				continue_previous_session = false;
			} else {
				stream_offset = ss->Tell();
			}
			/** 1. If there's no other tuple within same batch and no parsing error in current iteration,
			 *  there're 2 scenarios:
			 *     - last line is ended with rows separator
			 *     - last line is not ended with rows separator
			 *     Just check for the end of batch in case if complete row is parsed in this session
			 *
			 *  2. If the row separator is LF, the offset is calculated differently
			 */
			int delta = remaining_len - stream_offset;
			bool use_direct_offset = ( (tuple_delim_ != '\n') || (delta == 0) || ((delta == 1) && ss->Peek() == tuple_delim_));
			offset = error ? stream_offset :
					(m_next_tuple_start != -1 ? m_next_tuple_start : (use_direct_offset ? stream_offset : stream_offset - 1));

			// we increase number of processed tuples only in case if there were no parse error
			// Number of tuples = number of really completed tuples
			// check also whether at least some columns were materialized.
			// don't count empty row ({}).
			if((!error && (m_schema_defined || column_idx_ != 0)) || (m_schema_defined && m_number_of_materialized_fields == schema_size)
					|| (m_schema_defined && m_number_of_materialized_fields && m_messageHandler->currentArray() &&
							m_messageHandler->currentArray()->children == m_record_idx_in_json_collection + 1)){
				// if the schema is defined and no one column was materialized during parser session,
				// no tuple materialized in current record, shift forward the data buffer and continue
				if(m_schema_defined){
					if(!m_number_of_materialized_fields)
						goto label; // no tuple found

				}
				// fill remained columns for this tuple
				FillColumns<false>(0, NULL, num_fields, field_locations);

				// point to next record
				row_end_locations[m_num_tuples] = (*byte_buffer_ptr + offset);
                                
				// say new tuple is added
                reportNewTuple();
			}
			label :

		    remaining_len -= offset;

			// shift buffer to offset:
			*byte_buffer_ptr += offset;
			*next_row_start = *byte_buffer_ptr;

			// assign collected number of tuples
			*num_tuples = m_num_tuples;

			if (*num_tuples == max_tuples) {
				if (last_row_delim_offset_ == remaining_len) last_row_delim_offset_ = 0;
				return Status::OK;
			}

		}
	}
    return Status::OK;
}

void JsonDelimitedTextParser::addColumnInternal(int len, char** data, int* num_fields,
		FieldLocation* field_locations, PrimitiveType type, const std::string& key, bool flag ){

	/** Flows:
	 * Scenario 1. Only when the schema is defined. The column is NULL (so tuple materialization finalization is requested, with zero values).
	 *             If so, just fill the remained tuple with non-set values using tuple bitmap.
	 *
	 * Scenario 2.
	 * 1. Check the column arrived should be materialized.
	 * 2. If so, which tuple it belongs to?
	 *
	 *    2.1 Check object that encloses this column. If the object's array index = -1,
	 *    the object does not belong to array on its hierarchy.
	 *    In this case, the tuple is distinguished by rapidjson parser session completion (if at least 1 column
	 *    was materialzed during parsing session).
	 *
	 *    2.2 Column's enclosing Object's has array index <> -1. This means the object is a part of some array
	 *    and we need to track the index of this object within that array to understand tuples boundaries.
	 *
	 * Scenario 3. No schema defined. Treat the single JSON record as the tuple.
	 */

	/** Scenario 1 : Handle scenario of dummy columns addition.
	 *  Examine bitmap for placeholder for materialized column, construct
	 *  dummy fields accordingly */
	if(m_schema_defined && *data == NULL && len == 0 && key.empty()){
		// if all fields were materialized, just do nothing
		if(m_number_of_materialized_fields == schema_size){
			// set the column index to fields size so will not reach zero-filling flow again
			column_idx_ = num_cols_;
			return;
		}
		typedef boost::unordered_map<std::string, SchemaMapping> HASH;
		BOOST_FOREACH( HASH::value_type& v, m_schema ) {
			if(v.second.column_idx == -1)
				continue;
			if(!get_bit(m_tuple, v.second.column_idx)){
				// bit is not set, so construct the dummy and mark the bit:
				field_locations[*num_fields].len         = len;
				field_locations[*num_fields].start       = *data;
				field_locations[*num_fields].type        = type;

				// assign llvm column index
				field_locations[*num_fields].idx = v.second.llvm_tuple_idx;

				// set the bit busy in bitmap
				set_bit(m_tuple, v.second.column_idx);

				// number of materialized fields is increased
				++(*num_fields);

				// and say we materialized yet another column for current tuple
				++m_number_of_materialized_fields;
			}
		}
		// set the column index to fields size so will not reach zero-filling flow again
		column_idx_ = num_cols_;
		// and go out
		return;
	}

    int index = 0;
	if(m_schema_defined){
		m_mapping = m_schema[key];
		index = m_mapping.llvm_tuple_idx;
	}
	else
		index = column_idx_ + 1;

    /** Scenario 2.1
	 * Go if the current column should be materialized.
	 * For schema-defined tables, column should be also defined in schema. */
	if (ReturnCurrentColumn()) {
		if(m_schema_defined){
			// Scenario 2.2 - checking the column enclosing object - whether it is part of any array?
			if(m_messageHandler->currentObject()->index == -1){
				/** Scenario 2.2.1 - the object is not the part of an array.
                 * Just fill the field locations and set the busy bit in tuple bitmap,
				 * for plain schema (no mapping to JSON collection's elements)
				 * the tuple is completed when rapidjson parsing session completes. */
				field_locations[*num_fields].len         = len;
				field_locations[*num_fields].start       = *data;
				field_locations[*num_fields].type        = type;

				// set the bit busy in bitmap
				set_bit(m_tuple, m_mapping.column_idx);

				// and say we materialized yet another column for current tuple
				++m_number_of_materialized_fields;

				// specify the index of column within the table schema
				field_locations[*num_fields].idx = index;

				// number of materialized fields is increased
				++(*num_fields);
                ++column_idx_;
				return;
			}

			/** Scenario 2.2.2. Column belongs to the object that is the part of some array.
			 *  Thus, we need to understand the index of the column's record - within the enclosing array -
			 *  to understand whether we need to report new tuple (arrived column belong to a new tuple)
			 *  or update current tuple (arrived column belong to the tuple we currently handle).
			 *  Underlying JSON parser reset the array index when the array is completed.
			 * */
			if(m_record_idx_in_json_collection != m_messageHandler->currentObject()->index){
				// if there were some materialized fields already from the previous tuple,
				// complete that tuple and report it to the number of completions.
				// This works for cases except continuation, when the part of this tuple was constructed already in the previous batch.
				// For continuation case, consume "continuation" flag.
				if(m_number_of_materialized_fields && !m_continiation_flag){
					// we step into new tuple. Report this and handle empty slots:
					// if position bit is busy in the tuple bitmap, report new tuple
					// fill remained columns for this tuple
					FillColumns<false>(0, NULL, num_fields, field_locations);
					reportNewTuple();
				}
				else
					m_record_idx_in_json_collection = m_messageHandler->currentObject()->index;
			}
			m_continiation_flag = false;
			// set the bit busy in bitmap for newly arrived tuple
			set_bit(m_tuple, m_mapping.column_idx);
			// increase the number of materialized fields for current tuple:
			++m_number_of_materialized_fields;
		}

		/** Scenario 2.2.2 finalization and Scenario 3 (schema is not defined) */
		field_locations[*num_fields].len         = len;
		field_locations[*num_fields].start       = *data;
		field_locations[*num_fields].type        = type;

		// specify the index of column within the table schema
		field_locations[*num_fields].idx = index;

		// number of materialized fields is increased
		++(*num_fields);

		++column_idx_;
	}
}

void JsonDelimitedTextParser::reportNewTuple(){
	if(m_schema_defined){
		// clear tuple parse progress bitmap
		bitset_clear(m_tuple);

		// reset number of materialized fields within this tuple:
		m_number_of_materialized_fields = 0;

		// save the current object index within the array (if any).
		// -1 means no enclosing array for current object
		m_record_idx_in_json_collection = m_messageHandler->currentObject() != NULL ? m_messageHandler->currentObject()->index : -1;
	}

	// reset the "current column index"
	column_idx_ = num_partition_keys_;
	++m_num_tuples;
}

bool JsonDelimitedTextParser::ReturnCurrentColumn() const {
    // 1. Current column index should be less than expected number of columns (to not overflow the
	// preallocated buffer for metadata).
	// 2. Column that is parsed currently should be configured in schema mapping (so m_idx > 0)
	// 3. Current column index should be configured as requested for materialization.
	return m_schema_defined ? ( this->m_mapping.defined() && (m_mapping.column_idx < num_cols_) && is_materialized_col_[m_mapping.column_idx] ) :
			( column_idx_ < num_cols_ && is_materialized_col_[column_idx_] );
}

#include <algorithm>

struct  slot_asc_descriptor_sort
{
    inline bool operator() (const SlotDescriptor* slot1, const SlotDescriptor* slot2)
    {
        return (slot1->col_pos() < slot2->col_pos());
    }
};

void JsonDelimitedTextParser::setupSchemaMapping(const std::vector<SlotDescriptor*>& schema){
    schema_size = schema.size();

	if(schema_size == 0)
		return;

        int idx = 1;

        // sort arrived schema in order to contain slots in the order they appear in the original table schema (basing on col_pos()):
        std::vector<SlotDescriptor*> sorted_schema =  schema;
        std::sort(sorted_schema.begin(), sorted_schema.end(), slot_asc_descriptor_sort());
	// populate schema with the mapping from JSON key's fully qualified name to
	// column index within the table schema
	for(std::vector<SlotDescriptor*>::const_iterator it = sorted_schema.begin(); it != sorted_schema.end(); ++it){
		SchemaMapping mapping((*it)->col_pos(), idx++);
		m_schema[(*it)->nested_path()] = mapping;
	}

	// allocate the bitmap representing tuple parse progress:
	m_tuple = bitset_alloc(schema_size ? schema_size : 1);

	// and zero it:
	bitset_clear(m_tuple);

	// now the schema is defined:
	m_schema_defined = true;
}

}
