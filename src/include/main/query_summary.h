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

struct VectorSearchSummary {
    double vectorSearchTime = 0;
    uint64_t distanceComputations = 0;
    double distanceComputationsTime = 0;
    uint64_t listNbrsCalls = 0;
    double listNbrsCallsTime = 0;
    uint64_t oneHopCalls = 0;
    uint64_t twoHopCalls = 0;
    uint64_t dynamicTwoHopCalls = 0;
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

    KUZU_API VectorSearchSummary getVectorSearchSummary() const;

    void setPreparedSummary(PreparedSummary preparedSummary_);

    /**
     * @return true if the query is executed with EXPLAIN.
     */
    bool isExplain() const;

private:
    double executionTime = 0;
    VectorSearchSummary vectorSearchSummary;
    PreparedSummary preparedSummary;
};

} // namespace main
} // namespace kuzu
