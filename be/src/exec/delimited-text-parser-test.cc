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

#include <string>
#include <gtest/gtest.h>

#include "exec/delimited-text-parser-test-fixtures.hpp"

#include "util/cpu-info.h"

using namespace std;

namespace impala {

void DelimtedTextParserTest::validate(const string& data,
    int expected_offset, char tuple_delim, int expected_num_tuples,
    int expected_num_fields) {
  m_parser->parserReset();
  char* data_ptr = const_cast<char*>(data.c_str());
  int remaining_len = data.size();
  int offset = m_parser->FindFirstInstance(data_ptr, remaining_len);
  
  EXPECT_EQ(offset, expected_offset) << data;
  if (offset == -1) return;

  EXPECT_GE(offset, 1) << data;
  EXPECT_LT(offset, data.size()) << data;
  EXPECT_EQ(data[offset - 1], tuple_delim) << data;

  data_ptr += offset;
  remaining_len -= offset;

  char* row_end_locs[100];
  vector<FieldLocation> field_locations(100);
  int num_tuples = 0;
  int num_fields = 0;
  char* next_column_start;
  Status status = m_parser->ParseFieldLocations(
      100, remaining_len, &data_ptr, &row_end_locs[0], &field_locations[0], &num_tuples,
      &num_fields, &next_column_start);
  EXPECT_EQ(num_tuples, expected_num_tuples) << data;
  EXPECT_EQ(num_fields, expected_num_fields) << data;
}

void DelimtedTextParserTest::validate(const std::string& data,
	    int expected_offset, char tuple_delim, int expected_num_tuples,
	    int expected_num_fields, int expected_incompletes, bool continuation) {

	m_parser->parserReset(!continuation);
	char* data_ptr = const_cast<char*>(data.c_str());
	int remaining_len = data.size();
	int offset = continuation ? expected_offset : m_parser->FindFirstInstance(data_ptr, remaining_len);

	EXPECT_EQ(offset, expected_offset) << data;
	if (offset == -1) return;

	EXPECT_LT(offset, data.size()) << data;

	char* row_end_locs[100];
	std::vector<FieldLocation> field_locations(100);
	int num_tuples = 0;
	int num_fields = 0;
	char* next_column_start;

	Status ret = m_parser->ParseFieldLocations(
	      100, remaining_len, &data_ptr, &row_end_locs[0], &field_locations[0], &num_tuples,
	      &num_fields, &next_column_start);

	std::cout << "Fields parsed.\n";
	EXPECT_EQ(num_tuples, expected_num_tuples) << data;
	EXPECT_EQ(num_fields, expected_num_fields) << data;
}

TEST_F(DelimtedTextParserTest, BasicRawParserTest) {
	std::cout << "RawDelimitedTextParser, Basics" << std::endl;
  const char TUPLE_DELIM = '|';
  const char FIELD_DELIM = ',';
  const char COLLECTION_DELIM = ',';
  const char ESCAPE_CHAR = '@';

  const int NUM_COLS = 1;

  // Test without escape
  reset(RAW, NUM_COLS, TUPLE_DELIM, FIELD_DELIM, COLLECTION_DELIM);

  std::cout << "Basics : running \"no escape\" parser." << std::endl;
  // Note that only complete tuples "count"
  validate("no_delims", -1, TUPLE_DELIM, 0, 0);
  validate("abc||abc", 4, TUPLE_DELIM, 1, 1);
  validate("|abcd", 1, TUPLE_DELIM, 0, 0);
  validate("a|bcd", 2, TUPLE_DELIM, 0, 0);
  
  // Test with escape char
  reset(RAW, NUM_COLS, TUPLE_DELIM, FIELD_DELIM, COLLECTION_DELIM, ESCAPE_CHAR);

  std::cout << "Basics : running \"escape\" parser." << std::endl;
  validate("a@|a|bcd", 5, TUPLE_DELIM, 0, 0);
  validate("a@@|a|bcd", 4, TUPLE_DELIM, 1, 1);
  validate("a@@@|a|bcd", 7, TUPLE_DELIM, 0, 0);
  validate("a@@@@|a|bcd", 6, TUPLE_DELIM, 1, 1);
  validate("a|@@@|a|bcd", 2, TUPLE_DELIM, 1, 1);

  // // The parser doesn't support this case.  
  // // TODO: update test when it is fixed
  // validate("@|no_delims", -1, TUPLE_DELIM);

  // Test null characters
  const string str1("\0no_delims", 10);
  const string str2("ab\0||abc", 8);
  const string str3("\0|\0|\0", 5);
  const string str4("abc|\0a|abc", 10);
  const string str5("\0|aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 32);
  validate(str1, -1, TUPLE_DELIM, 0, 0);
  validate(str2, 4, TUPLE_DELIM, 1, 1);
  validate(str3, 2, TUPLE_DELIM, 1, 1);
  validate(str4, 4, TUPLE_DELIM, 1, 1);
  validate(str5, 2, TUPLE_DELIM, 0, 0);

  const string str6("\0@|\0|\0", 6);
  const string str7("\0@@|\0|\0", 6);
  const string str8("\0@\0@|\0|\0", 8);
  const string str9("\0@||aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 34);
  validate(str6, 5, TUPLE_DELIM, 0, 0);
  validate(str7, 4, TUPLE_DELIM, 1, 1);
  validate(str8, 7, TUPLE_DELIM, 0, 0);
  validate(str9, 4, TUPLE_DELIM, 0, 0);
}

TEST_F(DelimtedTextParserTest, FieldsRawParserTest) {
	std::cout << "RawDelimitedTextParser, Fields" << std::endl;
  const char TUPLE_DELIM = '|';
  const char FIELD_DELIM = ',';
  const char COLLECTION_DELIM = ',';
  const char ESCAPE_CHAR = '@';

  const int NUM_COLS = 2;

  reset(RAW, NUM_COLS, TUPLE_DELIM, FIELD_DELIM, COLLECTION_DELIM);

  std::cout << "Basics : running \"no escape\" parser." << std::endl;

  validate("a,b|c,d|e,f", 4, TUPLE_DELIM, 1, 3);
  validate("b|c,d|e,f", 2, TUPLE_DELIM, 1, 3);
  validate("a,|c,d|", 3, TUPLE_DELIM, 1, 2);
  validate("a,|c|e", 3, TUPLE_DELIM, 1, 2);

  const string str10("a,\0|c,d|e", 9);
  validate(str10, 4, TUPLE_DELIM, 1, 2);

  reset(RAW, NUM_COLS, TUPLE_DELIM, FIELD_DELIM, COLLECTION_DELIM, ESCAPE_CHAR);

  std::cout << "Basics : running \"escape\" parser." << std::endl;

  validate("a,b|c,d|e,f", 4, TUPLE_DELIM, 1, 3);
  validate("a,@|c|e,f", 6, TUPLE_DELIM, 0, 1);
  validate("a|b,c|d@,e", 2, TUPLE_DELIM, 1, 2);
}

TEST_F(DelimtedTextParserTest, Batch_0_no_delimiters) {
	char TUPLE_DELIM = '|';

	reset(JSON, -1, TUPLE_DELIM);
	validate("{\"no_delims\":100}", -1, TUPLE_DELIM, 0, 0, 0, false);
}

/** Scenario 1:
 *  Batch 1 finished with the JSON field's key, only part of it.
 *
 *  Batch 2 contains remainder for JSON record started in batch 1 and contains a part of JSON
 *  record where the integer value is specified and is not followed by neither "," nor "}".
 *  This last field is expected to not be added in batch 2, because we did not see JSON field / object
 *  separator yet.
 *
 *  Batch 3 contains continuation for JSON record started in batch 2. Remainder starts with JSON
 *  field separator ",". Continuation is truncated as well, this time integer value is
 *  truncated in the middle, thus, batch 3 contains only part of integer value.
 *  This partial value should not be consumed as we are not sure we have read it completely.
 *
 *  Batch 4 contains remainder for JSON record started in batch 2. It starts with
 *  remainder of integer value which was introduced in Batch 3.
 *  Batch 4 is finished with a very small JSON record which is written completely.
 */
TEST_F(DelimtedTextParserTest, Plain_JSON_simple_fields_truncated_batches_1) {
	char TUPLE_DELIM = '|';

	reset(JSON, 5, TUPLE_DELIM);

	// say 1 incompleted row is expected
	validate("|{\"field1\":120, \"field2\":\"text\",\"fi", 1, TUPLE_DELIM, 0, 2, 1, false);
	/** say 1 new incomplete in Batch 2. Here should be 1 completed row (which was started in Batch 1)
	 * and 3 columns as an output:
	 * 1 which is completed from previous batch, 1 which finalizes the record we started in Batch 1
	 * and 1 which was not materialized for current JSON record.
	 * Column from new JSON record is not taken into account as it is not completed
	 */
	validate("eld8\":360, \"field9\":\"hey\"}|{\"field10\":20", 0, TUPLE_DELIM, 1, 3, 1, true);

	/** Here, we should have 0 completed rows and 3 columns, first column is the completion of
	 *  Batch 2. Last column is non-completed and is not counted
	 */
	validate(",\"field11\":\"some text\",\"field12\":1,\"field13\":12", 0, TUPLE_DELIM, 0, 3, 1, true);

	/**
	 * Here, we have 2 rows as a result, 1 is completion of Batch 2 and 3, and one if completely found in
	 * current batch.
	 * Columns : 7.
	 * 2 columns in record we started in Batch 2 and continued in Batch 3;
	 * 5 columns are from last completed record, 1 is materialized and 4 filled with defaults
	 */
	validate("50,\"field14\":\"hey\"}|{\"field15\":20}", 0, TUPLE_DELIM, 2, 7, 0, true);
}

/** Scenario 2:
 * Batch 1 finished with the JSON fields separator ",".
 * Batch 2 contains the remainder for the record and partially new record.
 * Batch 3 contains the remainder for JSON from batch 2
*/
TEST_F(DelimtedTextParserTest, Plain_JSON_simple_fields_truncated_batches_2) {
	char TUPLE_DELIM = '|';

    reset(JSON, 2, TUPLE_DELIM);

    validate("|{\"field1\":120, \"field2\":\"text\",\"", 1, TUPLE_DELIM, 0, 2, 1, false);

    /** Batch 2 contains remainder for Record 1, 2 extra fields that should not be materialized,
     * therefore, they are not counted.
     * Rows = 1 (Record 1 is completed)
     * Columns = 1
     * 0 (columns skipped from Record 1 materialization) + 1 (column from Record 2)
     */
    validate("field3\":200, \"field4\":\"bye\"}|{\"new\":\"text\",", 0, TUPLE_DELIM, 1, 1, 1, true);

    /** Batch 3 contains Record 2 completion.
     *  Record 2 contains 3 columns, from them only 2 first should be materialized, therefore we do not count
     *  and extra column in Batch 2
     */
    validate("\"field8\":360, \"field9\":\"hey\"}", 0, TUPLE_DELIM, 1, 1, 0, true);
}

/** Scenario 3:
* Batch 1 does not contain complete record, is finished with the JSON fields separator ",".
* Batch 2 contains the remainder for the record and also extra field which should not be materialized.
* Then part of new record is started and is truncated missing field separator ",".
* Batch 3 contains the remainder for JSON from batch 2, starting with ",".
* Then empty record {}. Then completed new record with 1 field from 2. Then row separator
*/
TEST_F(DelimtedTextParserTest, Plain_JSON_simple_fields_truncated_batches_3) {
	char TUPLE_DELIM = '|';

    reset(JSON, 2, TUPLE_DELIM);
	validate("|{\"field1\":\"te\", \"field2\":\"text\",", 1, TUPLE_DELIM, 0, 2, 1, false);

	/** Column we completed from Batch 1 should not be materialized, therefore we do not count it.
	 * But we say we completed the 1 tuple on this iteration.
	 * Column from record 2 is not followed with field separator but should count as it is string type
	 * and trailing \" is detected.
	*/
	validate("\"field3\":\"val\"}|{\"field1\":\"data\"", 16, TUPLE_DELIM, 1, 1, 1, true);

	/** Record 2 is completed. Then empty record, we do not count.
	 * Then complete record with 1 materialized column. Batch is completed with a row separator
	 */
	validate(",\"field2\":\"value\"}|{}|{\"field1\":\"value\"}|", 19, TUPLE_DELIM, 2, 3, 0, true);

	/** The only complete record with a single materialized field
	 */
	validate("{\"field1\" : \"test\"}", 0, TUPLE_DELIM, 1, 2, 0, true);
}

/** Scenario 4: JSON with array field.
 * Batch 1 : contains start of record. array-field is truncated with array members separator ",".
 *
 * Batch 2 : completes Record 1, array field value. Starts Record 2 which is truncated and contains only field name
 * and miss "expect value" symbol ":".
 *
 * Batch 3 : starts with continuation to Record 2, ":" symbol. Completes Record 2. Starts Record 3
 * which is truncated with ":", so, value is expected next
 *
 * Batch 4: completes Record 3 and materialize all Record 3 columns. Starts Record 4.
 * Record 4 is truncated in the middle of first of array field's values.
 *
 * Batch 5: Record 4 is completed. Record 5 starts. Record 5 is truncated in the middle of second
 * of array field's values.
 *
 * Batch 6: contains continuation for Record's 5 array field values.
 * Is truncated without array enclosing "]" appear
 *
 * Batch 7 : Record 5 is completed. Record 6 is started and contains all content except enclosing "}"
 * which should mark the end of tuple.
 */
TEST_F(DelimtedTextParserTest, Plain_JSON_array_fields_truncated_batches) {
	char TUPLE_DELIM = '|';

    reset(JSON, 8, TUPLE_DELIM);
    /** Rows    : 0
     *  Columns : 2, one text and 1 flattened from array
    */
	validate("|{\"simple\":\"text\",\"arr\":[12,", 1, TUPLE_DELIM, 0, 2, 2, false);

	/** Record 1 from batch 1 is completed.
	 * Rows = 1. Column #3 from record 1 flattened is completed.
	 * Record 2 contains field #1 completely and the array field is truncated.
	 * Columns = 7 :
	 * 1 (materialized from record 1, batch 1) + 5 (non-materialized from record 1, batch 1)
	 * + 1 (from incomplete record 2, batch 2)
	*/
	validate("14]}|{\"simple\":\"data\",\"arr\"", 5, TUPLE_DELIM, 1, 7, 1, true);

	/** Record 2 is completed, 2 fields are found.
	 *  Rows = 1.
	 *  Record 3 is truncated, expected array field value is missing.
	 *  Columns = 8
	 *  3 (materialized from Record 2) + 4 (non-materialized from Record 2)
	 *  + 1 (from incomplete Record 3)
	 */
	validate(":[10, 12, 14]}|{\"simple\":\"test\",\"arr\":", 15, TUPLE_DELIM, 1, 8, 1, true);

	/** Record 3 is completed and its fields all materialized.
	 *  Record 4 is truncated at the middle of array value, missed "," after array member.
	 *  Rows = 1 (completed Record 3)
	 *  Columns = 8
	 *  7 (Record 3) + 1 (start of Record 4)
	 */
	validate("[14,16,20,22,24,26,28]}|{\"simple\":\"value\",\"arr\":[1", 0, TUPLE_DELIM, 1, 8, 2, true);

	/** Record 4 is completed.
	 *  Record 5 is truncated at the middle of second array field's value.
	 *  Rows = 1 (completed Record 4)
	 *  Columns =  9
	 *  2 (materialized from Record 4) + 5 (non-materialized from Record 4)
	 *  + 2 (materialized from Record 5)
	 */
	validate("8,20]}|{\"simple\":\"sun\",\"arr\":[20,2", 0, TUPLE_DELIM, 1, 9, 2, true);

    /** Record 5 is continued and is not completed.
     * In this batch, only array's field values appear and there's no array enclosing "]".
     * Rows = 0
     * Columns = 2
     * 1 (completed from previous batch) + 1 (materialized from current batch)
     *
     */
	validate("4,25,26", 0, TUPLE_DELIM, 0, 2, 2, true);

	/**
	 * Record 5 is completed. Record 6 is started and contains all content except enclosing "}"
	 * which should mark the end of tuple.
	 *
	 * Rows = 1
	 * Columns = 7
	 * 1 (completed from Record 5 from batch 6) + 3 (non-materialized from Record 5 from Batch 6)
	 * + 3 (materialized from Record 6)
	 */
	validate("4]}|{\"simple\":\"star\",\"arr\":[26,28]", 0, TUPLE_DELIM, 1, 7, 1, true);

	/** finalization of Record 6, just formal.
	 * Rows = 1
	 * Columns = 5 (all of them are non-materialized partition columns)
	 */
	validate("}", 0, TUPLE_DELIM, 1, 5, 0, true);
}
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::CpuInfo::Init();
  return RUN_ALL_TESTS();
}

