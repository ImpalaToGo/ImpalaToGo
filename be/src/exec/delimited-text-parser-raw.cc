// Copyright 2014 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*
 * @file delimited-text-parser-raw.cc
 *
 * @brief Contains Raw Parser implementation of DelimitedTextParser.
 * Raw accepts "column" separator, "tuple" separator, "collection" separator
 * and performs escapes raw parsing.
 *
 * @date   Apr 9, 2015
 * @author elenav
 */

#include "exec/delimited-text-parser.inline.h"
#include "exec/delimited-text-parser-raw.inline.h"

using namespace impala;
using namespace std;

RawDelimitedTextParser::RawDelimitedTextParser(
	      int num_cols, int num_partition_keys, const bool* is_materialized_col,
	      char tuple_delim, char field_delim, char collection_item_delim,
	      char escape_char) :
	    		  DelimitedTextParser(num_cols, num_partition_keys, is_materialized_col, tuple_delim),
				  field_delim_(field_delim),
				  escape_char_(escape_char),
				  collection_item_delim_(collection_item_delim),
				  current_column_has_escape_(false),
				  last_char_is_escape_(false){

	// Escape character should not be the same as tuple or col delim unless it is the
	// empty delimiter.
	DCHECK(escape_char == '\0' || escape_char != tuple_delim);
	DCHECK(escape_char == '\0' || escape_char != field_delim);
	DCHECK(escape_char == '\0' || escape_char != collection_item_delim);

	// configure search characters registry
	setupSearchCharacters();
	// reset the parser
	parserResetInternal();
}

void RawDelimitedTextParser::setupSearchCharacters(){
	  // Initialize the sse search registers.
	  char search_chars[SSEUtil::CHARS_PER_128_BIT_REGISTER];
	  memset(search_chars, 0, sizeof(search_chars));

	  if (process_escapes_) {
	    search_chars[0] = escape_char_;
	    xmm_escape_search_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(search_chars));

	    // To process escape characters, we need to check if there was an escape
	    // character between (col_start,col_end).  The SSE instructions return
	    // a bit mask for 16 bits so we need to mask off the bits below col_start
	    // and after col_end.
	    low_mask_[0] = 0xffff;
	    high_mask_[15] = 0xffff;
	    for (int i = 1; i < 16; ++i) {
	      low_mask_[i] = low_mask_[i - 1] << 1;
	    }
	    for (int i = 14; i >= 0; --i) {
	      high_mask_[i] = high_mask_[i + 1] >> 1;
	    }
	  }
	  else {
		    memset(high_mask_, 0, sizeof(high_mask_));
		    memset(low_mask_, 0, sizeof(low_mask_));
	  }

	  if (tuple_delim_ != '\0') {
	    search_chars[num_delims_++] = tuple_delim_;
	    // Hive will treats \r (^M) as an alternate tuple delimiter, but \r\n is a
	    // single tuple delimiter.
	    if (tuple_delim_ == '\n') search_chars[num_delims_++] = '\r';
	    xmm_tuple_search_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(search_chars));
	  }

	  if (field_delim_ != '\0' || collection_item_delim_ != '\0') {
		  search_chars[num_delims_++] = field_delim_;
		  search_chars[num_delims_++] = collection_item_delim_;
	  }

	  DCHECK_GT(num_delims_, 0);
	  xmm_delim_search_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(search_chars));
}

void RawDelimitedTextParser::parserResetInternal(){
	  current_column_has_escape_ = false;
	  last_char_is_escape_ = false;
}

Status RawDelimitedTextParser::ParseFieldLocations(int max_tuples, int64_t remaining_len,
    char** byte_buffer_ptr, char** row_end_locations,
    FieldLocation* field_locations,
    int* num_tuples, int* num_fields, char** next_column_start) {

  // Start of this batch.
  *next_column_start = *byte_buffer_ptr;

  // If there was a '\r' at the end of the last batch, set the offset to
  // just before the beginning. Otherwise make it invalid.
  if (last_row_delim_offset_ == 0) {
    last_row_delim_offset_ = remaining_len;
  } else {
    last_row_delim_offset_ = -1;
  }

  if (CpuInfo::IsSupported(CpuInfo::SSE4_2)) {
    if (process_escapes_) {
      ParseSse<true>(max_tuples, &remaining_len, byte_buffer_ptr, row_end_locations,
          field_locations, num_tuples, num_fields, next_column_start);
    } else {
      ParseSse<false>(max_tuples, &remaining_len, byte_buffer_ptr, row_end_locations,
          field_locations, num_tuples, num_fields, next_column_start);
    }
  }

  if (*num_tuples == max_tuples) return Status::OK;

  // Handle the remaining characters
  while (remaining_len > 0) {
    bool new_tuple = false;
    bool new_col = false;
    unfinished_tuple_ = true;

    if (!last_char_is_escape_) {
      if (tuple_delim_ != '\0' && (**byte_buffer_ptr == tuple_delim_ ||
           (tuple_delim_ == '\n' && **byte_buffer_ptr == '\r'))) {
        new_tuple = true;
        new_col = true;
      } else if (**byte_buffer_ptr == field_delim_
                 || **byte_buffer_ptr == collection_item_delim_) {
        new_col = true;
      }
    }

    if (process_escapes_ && **byte_buffer_ptr == escape_char_) {
      current_column_has_escape_ = true;
      last_char_is_escape_ = !last_char_is_escape_;
    } else {
      last_char_is_escape_ = false;
    }

    if (new_tuple) {
      if (last_row_delim_offset_ == remaining_len && **byte_buffer_ptr == '\n') {
        // If the row ended in \r\n then move to the \n
        ++*next_column_start;
      } else {
        AddColumn<true>(*byte_buffer_ptr - *next_column_start,
            next_column_start, num_fields, field_locations);
        FillColumns<false>(0, NULL, num_fields, field_locations);
        column_idx_ = num_partition_keys_;
        row_end_locations[*num_tuples] = *byte_buffer_ptr;
        ++(*num_tuples);
      }
      unfinished_tuple_ = false;
      last_row_delim_offset_ = **byte_buffer_ptr == '\r' ? remaining_len - 1 : -1;
      if (*num_tuples == max_tuples) {
        ++*byte_buffer_ptr;
        --remaining_len;
        if (last_row_delim_offset_ == remaining_len) last_row_delim_offset_ = 0;
        return Status::OK;
      }
    } else if (new_col) {
      AddColumn<true>(*byte_buffer_ptr - *next_column_start,
          next_column_start, num_fields, field_locations);
    }

    --remaining_len;
    ++*byte_buffer_ptr;
  }

  // For formats that store the length of the row, the row is not delimited:
  // e.g. Sequence files.
  if (tuple_delim_ == '\0') {
    DCHECK_EQ(remaining_len, 0);
    AddColumn<true>(*byte_buffer_ptr - *next_column_start,
        next_column_start, num_fields, field_locations);
    FillColumns<false>(0, NULL, num_fields, field_locations);
    column_idx_ = num_partition_keys_;
    ++(*num_tuples);
    unfinished_tuple_ = false;
  }
  return Status::OK;
}

bool RawDelimitedTextParser::process_escapes(int start, const char* buffer){
	// Scan backwards for escape characters.  We do this after
	// finding the tuple break rather than during the (above)
	// forward scan to make the forward scan faster.  This will
	// perform worse if there are many characters right before the
	// tuple break that are all escape characters, but that is
	// unlikely.
	int num_escape_chars = 0;
	int before_tuple_end = start - 2;
	// TODO: If scan range is split between escape character and tuple delimiter,
	// before_tuple_end will be -1. Need to scan previous range for escape characters
	// in this case.
	for (; before_tuple_end >= 0; --before_tuple_end) {
		if (buffer[before_tuple_end] == escape_char_) {
			++num_escape_chars;
		} else {
			break;
		}
	}

	// TODO: This sucks.  All the preceding characters before the tuple delim were
	// escape characters.  We need to read from the previous block to see what to do.
	if (before_tuple_end < 0) {
		static bool warning_logged = false;
		if (!warning_logged) {
			LOG(WARNING)<< "Unhandled code path.  This might cause a tuple to be "
			<< "skipped or repeated.";
			warning_logged = true;
		}
	}

	// An even number of escape characters means they cancel out and this tuple break
	// is *not* escaped.
	if (num_escape_chars % 2 != 0)
		return true;
	return false;
}

void RawDelimitedTextParser::addColumnInternal(int len, char** next_column_start, int* num_fields,
		FieldLocation* field_locations, PrimitiveType type, bool process_escapes ){
	  if (ReturnCurrentColumn()) {
	    // Found a column that needs to be parsed, write the start/len to 'field_locations'
	    field_locations[*num_fields].start = *next_column_start;
	    if (process_escapes && current_column_has_escape_) {
	      field_locations[*num_fields].len = -len;
	    } else {
	      field_locations[*num_fields].len = len;
	    }
	    ++(*num_fields);
	  }
	  if (process_escapes) current_column_has_escape_ = false;
}
