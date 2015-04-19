// Copyright 2012 Cloudera Inc.
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


#ifndef IMPALA_EXEC_DELIMITED_TEXT_PARSER_INLINE_H
#define IMPALA_EXEC_DELIMITED_TEXT_PARSER_INLINE_H

#include "delimited-text-parser.h"
#include "util/cpu-info.h"
#include "util/sse-util.h"

namespace impala {

template <bool process_escapes>
inline void DelimitedTextParser::AddColumn(int len, char** next_column_start,
    int* num_fields, FieldLocation* field_locations, PrimitiveType type) {
	addColumnInternal(len, next_column_start, num_fields, field_locations, type, process_escapes);
	++column_idx_;
}

template <bool process_escapes>
void inline DelimitedTextParser:: FillColumns(int len, char** last_column,
    int* num_fields, FieldLocation* field_locations) {
  // Fill in any columns missing from the end of the tuple.
  char* dummy = NULL;
  if (last_column == NULL) last_column = &dummy;
  while (column_idx_ < num_cols_) {
    AddColumn<process_escapes>(len, last_column, num_fields, field_locations);
    // The rest of the columns will be null.
    last_column = &dummy;
    len = 0;
  }
}

// Simplified version of ParseSSE which does not handle tuple delimiters.
template <bool process_escapes>
inline void DelimitedTextParser::ParseSingleTuple(int64_t remaining_len, char* buffer,
    FieldLocation* field_locations, int* num_fields) {
	parseSingleTupleInternal(remaining_len, buffer, field_locations, num_fields, process_escapes);
}

}

#endif

