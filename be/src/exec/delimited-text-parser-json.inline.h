/*
 * @file delimited-text-parser-json.inline.h
 *
 * @brief Contains inlined json parser implementations of DelimitedTextParser interface
 *
 * @date   Apr 9, 2015
 * @author elenav
 */

#ifndef SRC_EXEC_DELIMITED_TEXT_PARSER_JSON_INLINE_H_
#define SRC_EXEC_DELIMITED_TEXT_PARSER_JSON_INLINE_H_

#include "delimited-text-parser-json.h"
#include "util/cpu-info.h"
#include "util/sse-util.h"

namespace impala {

inline void JsonDelimitedTextParser::parseSingleTupleInternal(int64_t remaining_len, char* buffer,
    FieldLocation* field_locations, int* num_fields, const bool process_escapes_flag) {
}

void inline JsonDelimitedTextParser::printColumn(int index, FieldLocation* field_locations){
	FieldLocation meta = field_locations[index];
	std::cout << "Fields[" << index << "] = \"" << meta.start << "\"" << std::endl;
}

}



#endif /* SRC_EXEC_DELIMITED_TEXT_PARSER_JSON_INLINE_H_ */
