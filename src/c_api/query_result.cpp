#include "main/query_result.h"

#include "c_api/helpers.h"
#include "c_api/kuzu.h"

using namespace kuzu::main;
using namespace kuzu::common;
using namespace kuzu::processor;

void kuzu_query_result_destroy(kuzu_query_result* query_result) {
    if (query_result == nullptr) {
        return;
    }
    if (query_result->_query_result != nullptr) {
        delete static_cast<QueryResult*>(query_result->_query_result);
    }
    delete query_result;
}

bool kuzu_query_result_is_success(kuzu_query_result* query_result) {
    return static_cast<QueryResult*>(query_result->_query_result)->isSuccess();
}

char* kuzu_query_result_get_error_message(kuzu_query_result* query_result) {
    auto error_message = static_cast<QueryResult*>(query_result->_query_result)->getErrorMessage();
    if (error_message.empty()) {
        return nullptr;
    }
    return convertToOwnedCString(error_message);
}

uint64_t kuzu_query_result_get_num_columns(kuzu_query_result* query_result) {
    return static_cast<QueryResult*>(query_result->_query_result)->getNumColumns();
}

char* kuzu_query_result_get_column_name(kuzu_query_result* query_result, uint64_t index) {
    auto column_names = static_cast<QueryResult*>(query_result->_query_result)->getColumnNames();
    if (index >= column_names.size()) {
        return nullptr;
    }
    return convertToOwnedCString(column_names[index]);
}

kuzu_logical_type* kuzu_query_result_get_column_data_type(
    kuzu_query_result* query_result, uint64_t index) {
    auto column_data_types =
        static_cast<QueryResult*>(query_result->_query_result)->getColumnDataTypes();
    if (index >= column_data_types.size()) {
        return nullptr;
    }
    auto column_data_type = column_data_types[index];
    auto* column_data_type_c = (kuzu_logical_type*)malloc(sizeof(kuzu_logical_type));
    column_data_type_c->_data_type = new LogicalType(column_data_type);
    return column_data_type_c;
}

uint64_t kuzu_query_result_get_num_tuples(kuzu_query_result* query_result) {
    return static_cast<QueryResult*>(query_result->_query_result)->getNumTuples();
}

kuzu_query_summary* kuzu_query_result_get_query_summary(kuzu_query_result* query_result) {
    auto query_summary = static_cast<QueryResult*>(query_result->_query_result)->getQuerySummary();
    auto* query_summary_c = (kuzu_query_summary*)malloc(sizeof(kuzu_query_summary));
    query_summary_c->_query_summary = query_summary;
    return query_summary_c;
}

bool kuzu_query_result_has_next(kuzu_query_result* query_result) {
    return static_cast<QueryResult*>(query_result->_query_result)->hasNext();
}

kuzu_flat_tuple* kuzu_query_result_get_next(kuzu_query_result* query_result) {
    auto flat_tuple = static_cast<QueryResult*>(query_result->_query_result)->getNext();
    auto* flat_tuple_c = (kuzu_flat_tuple*)malloc(sizeof(kuzu_flat_tuple));
    flat_tuple_c->_flat_tuple = new std::shared_ptr<FlatTuple>(flat_tuple);
    return flat_tuple_c;
}

char* kuzu_query_result_to_string(kuzu_query_result* query_result) {
    return convertToOwnedCString(
        static_cast<QueryResult*>(query_result->_query_result)->toString());
}

void kuzu_query_result_write_to_csv(kuzu_query_result* query_result, const char* file_path,
    char delimiter, char escape_char, char new_line) {
    static_cast<QueryResult*>(query_result->_query_result)
        ->writeToCSV(file_path, delimiter, escape_char, new_line);
}

void kuzu_query_result_reset_iterator(kuzu_query_result* query_result) {
    static_cast<QueryResult*>(query_result->_query_result)->resetIterator();
}

struct ArrowSchema kuzu_query_result_get_arrow_schema(kuzu_query_result* query_result) {
    return *static_cast<QueryResult*>(query_result->_query_result)->getArrowSchema();
}

struct ArrowArray kuzu_query_result_get_next_arrow_chunk(
    kuzu_query_result* query_result, int64_t chunk_size) {
    return *static_cast<QueryResult*>(query_result->_query_result)->getNextArrowChunk(chunk_size);
}
