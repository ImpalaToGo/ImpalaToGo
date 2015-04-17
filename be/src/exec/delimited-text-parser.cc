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

#include "exec/delimited-text-parser.inline.h"

#include "exec/hdfs-scanner.h"
#include "util/cpu-info.h"

using namespace impala;
using namespace std;

DelimitedTextParser::DelimitedTextParser(
    int num_cols, int num_partition_keys, const bool* is_materialized_col, char tuple_delim) :
		num_delims_(0),
		process_escapes_(false),
		tuple_delim_(tuple_delim),
		last_row_delim_offset_(-1),
		num_cols_(num_cols),
		num_partition_keys_(num_partition_keys),
		is_materialized_col_(is_materialized_col),
		column_idx_(0),
		unfinished_tuple_(false){

}

void DelimitedTextParser::parserReset(bool hard) {
	last_row_delim_offset_ = -1;
	if(hard)
		column_idx_ = num_partition_keys_;
	parserResetInternal();
}


// Find the first instance of the tuple delimiter.  This will
// find the start of the first full tuple in buffer by looking for the end of
// the previous tuple.
int DelimitedTextParser::FindFirstInstance(const char* buffer, int len) {
  int tuple_start = 0;
  const char* buffer_start = buffer;
  bool found = false;

  // If the last char in the previous buffer was \r then either return the start of
  // this buffer or skip a \n at the beginning of the buffer.
  if (last_row_delim_offset_ != -1) {
    if (*buffer_start == '\n') return 1;
    return 0;
  }
  bool restart_escape_processing = false;
  while(restart_escape_processing){
	  found = false;

	  if (CpuInfo::IsSupported(CpuInfo::SSE4_2)) {
		  __m128i xmm_buffer, xmm_tuple_mask;
		  while (len - tuple_start >= SSEUtil::CHARS_PER_128_BIT_REGISTER) {
			  // TODO: can we parallelize this as well?  Are there multiple sse execution units?
			  // Load the next 16 bytes into the xmm register and do strchr for the
			  // tuple delimiter.
			  xmm_buffer = _mm_loadu_si128(reinterpret_cast<const __m128i *>(buffer));
			  xmm_tuple_mask = SSE4_cmpestrm(xmm_tuple_search_, 1, xmm_buffer,
					  SSEUtil::CHARS_PER_128_BIT_REGISTER,
					  SSEUtil::STRCHR_MODE);

			  int tuple_mask = _mm_extract_epi16(xmm_tuple_mask, 0);

			  if (tuple_mask != 0) {
				  found = true;
				  for (int i = 0; i < SSEUtil::CHARS_PER_128_BIT_REGISTER; ++i) {
					  if ((tuple_mask & SSEUtil::SSE_BITMASK[i]) != 0) {
						  tuple_start += i + 1;
						  buffer += i + 1;
						  std::cout << "SSE : found delimiter at " << tuple_start << "\n";
						  break;
					  }
				  }
				  break;
			  }
			  tuple_start += SSEUtil::CHARS_PER_128_BIT_REGISTER;
			  buffer += SSEUtil::CHARS_PER_128_BIT_REGISTER;
		  }
	  }
	  if (!found) {
		  std::cout << "SSE did not find delimiter, currently at " << tuple_start << "\n";
		  for (; tuple_start < len; ++tuple_start) {
			  char c = *buffer++;
			  if (c == tuple_delim_ || (c == '\r' && tuple_delim_ == '\n')) {
				  ++tuple_start;
				  found = true;
				  std::cout << "found delimiter, currently at " << tuple_start << "\n";
				  break;
			  }
		  }
	  }

	  if (!found)
		  return -1;
	  if (process_escapes_) {
		  std::cout << "Process escapes is requested. currently at " << tuple_start << "\n";
		  restart_escape_processing = process_escapes(tuple_start,
				  buffer_start);
	  }
  }

  if (tuple_start == len - 1 && buffer_start[tuple_start] == '\r') {
    // If \r is the last char we need to wait to see if the next one is \n or not.
	last_row_delim_offset_ = 0;
    return -1;
  }
  if (tuple_start < len && buffer_start[tuple_start] == '\n' &&
      buffer_start[tuple_start - 1] == '\r') {
    // We have \r\n, move to the next character.
    ++tuple_start;
  }
  std::cout << "Find first instance : tuple start = " << tuple_start << "\n";
  return tuple_start;
}

