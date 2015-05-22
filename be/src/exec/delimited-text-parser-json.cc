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
		m_tuple(NULL){

	// bind "column detected" handler to this parser to be handled here
	m_columnDetectedHandler = boost::bind(boost::mem_fn(&DelimitedTextParser::AddColumn<false>), this,
			_1, _2, _3, _4, _5, _6);

	// create the rapidjson events handler:
	m_messageHandler.reset(new JsonSAXParserEventsHandler(m_columnDetectedHandler, m_compundColumnDetectedhandler));

	// configure search characters registry
	setupSearchCharacters();

	// reset the parser
	parserResetInternal();
}

JsonDelimitedTextParser::~JsonDelimitedTextParser(){
	if(m_tuple != NULL)
		bitset_free(m_tuple);
	m_tuple = NULL;
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

void JsonDelimitedTextParser::parserResetInternal(){
	m_data_remainder_size = -1;

	// if there were buffer created for data reconstruction, release it:
	if(m_reconstructedRecordData != NULL){
		delete [] m_reconstructedRecordData;
		m_reconstructedRecordData = NULL;
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
	bool continue_previous_session = continuePreviousSession(byte_buffer_ptr, &remaining_len);
	if(continue_previous_session)
		m_messageHandler->reset(continue_previous_session, true);
	m_messageHandler->configure(field_locations, num_fields);

	// we always go and try to parse what we have in current batch.
	// we say we have new tuple completed when parser gives no error
	// so we will not miss any tuples
	bool initial_tuple_found_flag = true;

	// Handle batch data:
    while (remaining_len > 0) {
		bool new_tuple = initial_tuple_found_flag;

		// and reset the initial "tuple found" flag:
		initial_tuple_found_flag = false;
		size_t offset = 0;

		if(tuple_delim_ == '\n' && **byte_buffer_ptr == '\r'){
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
			new_tuple = true;
		}

		*next_row_start = *byte_buffer_ptr;

		// start to handle new tuple
		if (new_tuple) {
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
			if(!error && (column_idx_ != 0)){
				// fill remained columns for this tuple
				FillColumns<false>(0, NULL, num_fields, field_locations);

				if(m_schema_defined)
					// clear tuple parse progress bitset
					bitset_clear(m_tuple);

				// reset the "current column index"
				column_idx_ = num_partition_keys_;

				// point to next record
				row_end_locations[*num_tuples] = (*byte_buffer_ptr + offset);

                // now we can increase the number of processed tuples:
				++(*num_tuples);
			}
		    remaining_len -= offset;

			// shift buffer to offset:
			*byte_buffer_ptr += offset;
			*next_row_start = *byte_buffer_ptr;

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

	int index = 0;

	if(m_schema_defined){
		m_mapping = m_schema[key];
		index = m_mapping.llvm_tuple_idx;
	}
	else
		index = column_idx_ + 1;

    // if current column is materialized:
	if (ReturnCurrentColumn()) {               
		field_locations[*num_fields].len         = len;
		field_locations[*num_fields].start       = *data;
		field_locations[*num_fields].type        = type;

		if(m_schema_defined){
			if(len == 0 && *data == NULL){
				set_bit(m_tuple, m_mapping.column_idx);
			}
		}

		// specify the index of column within the table schema
		field_locations[*num_fields].idx = index;

		// number of materialized fields is increased
		++(*num_fields);
	}
	++column_idx_;
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
	LOG(INFO) << "Configuring schema mapping. Schema len = " << schema.size() << ".\n";

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
                LOG(INFO) << "schema mapping of \"" << (*it)->nested_path() << "\"; col_idx = " << (*it)->col_pos() << " to idx = " << idx << ".\n";
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
