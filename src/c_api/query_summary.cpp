#include "main/query_summary.h"

#include <cstdlib>

#include "c_api/kuzu.h"

using namespace kuzu::main;

void kuzu_query_summary_destroy(kuzu_query_summary* query_summary) {
    if (query_summary == nullptr) {
        return;
    }
    // The query summary is owned by the query result, so it should not be deleted here.
    query_summary->_query_summary = nullptr;
    free(query_summary);
}

double kuzu_query_summary_get_compiling_time(kuzu_query_summary* query_summary) {
    return static_cast<QuerySummary*>(query_summary->_query_summary)->getCompilingTime();
}

double kuzu_query_summary_get_execution_time(kuzu_query_summary* query_summary) {
    return static_cast<QuerySummary*>(query_summary->_query_summary)->getExecutionTime();
}
