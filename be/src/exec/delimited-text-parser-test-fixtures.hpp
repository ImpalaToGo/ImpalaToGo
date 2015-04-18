/*
 * @file delimited-text-json-parser-test-fixtures.hpp
 *
 * @brief contains test suite setup for JSON parser
 *
 * @date   Apr 16, 2015
 * @author elenav
 */

#ifndef SRC_EXEC_DELIMITED_TEXT_JSON_PARSER_TEST_FIXTURES_HPP_
#define SRC_EXEC_DELIMITED_TEXT_JSON_PARSER_TEST_FIXTURES_HPP_

#include <gtest/gtest.h>
#include <vector>
#include <string>

#include "exec/delimited-text-parser.inline.h"
#include "exec/delimited-text-parser-json.inline.h"

namespace impala{

/** Fixture for JSON parser tests */
class JsonParserTest : public ::testing::Test {
 protected:
	/** parser reference */
	JsonDelimitedTextParser* m_parser;

	/**mask to mark fields that should be materialized */
	bool* is_materialized_cols;

    static void SetUpTestCase() {
    	impala::InitGoogleLoggingSafe("Test_json_parser");
    	impala::InitThreading();
    }

	virtual void SetUp() { }

	virtual void TearDown() {
		if(m_parser != NULL){
			delete m_parser;
			m_parser = NULL;
		}
		if(is_materialized_cols != NULL){
			delete [] is_materialized_cols;
			is_materialized_cols = NULL;
		}
	}

	/** reset the local state */
	void reset(int num_cols, char tuple_delim){
		const int NUM_COLS = 10;
		is_materialized_cols = new bool[NUM_COLS];
		for (int i = 0; i < NUM_COLS; ++i) is_materialized_cols[i] = true;
		m_parser = new JsonDelimitedTextParser(num_cols, 0, is_materialized_cols, tuple_delim);
	}

	/** validate assumptions in regards to a batch */
	void validate(const std::string& data,
		    int expected_offset, char tuple_delim, int expected_num_tuples,
		    int expected_num_fields, int expected_incompletes, bool continuation = false);
};
}



#endif /* SRC_EXEC_DELIMITED_TEXT_JSON_PARSER_TEST_FIXTURES_HPP_ */
