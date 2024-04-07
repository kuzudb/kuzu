#pragma once

#include "common/api.h"
#include "kuzu_fwd.h"

namespace kuzu {
namespace main {

/**
 * @brief PreparedSummary stores the compiling time and query options of a query.
 */
struct PreparedSummary {
    double compilingTime = 0;
    common::StatementType statementType;
};

/**
 * @brief QuerySummary stores the execution time, plan, compiling time and query options of a query.
 */
class QuerySummary {
    friend class ClientContext;
    friend class benchmark::Benchmark;

public:
    /**
     * @return query compiling time in milliseconds.
     */
    KUZU_API double getCompilingTime() const;
    /**
     * @return query execution time in milliseconds.
     */
    KUZU_API double getExecutionTime() const;

    void setPreparedSummary(PreparedSummary preparedSummary_);

    /**
     * @return true if the query is executed with EXPLAIN.
     */
    bool isExplain() const;

private:
    double executionTime = 0;
    PreparedSummary preparedSummary;
};

} // namespace main
} // namespace kuzu
