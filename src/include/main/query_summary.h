#pragma once

#include "common/api.h"
#include "json_fwd.hpp"
#include "kuzu_fwd.h"
#include "plan_printer.h"

namespace kuzu {
namespace main {

/**
 * @brief PreparedSummary stores the compiling time and query options of a query.
 */
struct PreparedSummary {
    double compilingTime = 0;
    // Only used for printing by shell.
    bool isExplain = false;
    bool isProfile = false;
};

/**
 * @brief QuerySummary stores the execution time, plan, compiling time and query options of a query.
 */
class QuerySummary {
    friend class Connection;
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
    bool getIsProfile() const;
    std::ostringstream& getPlanAsOstream();
    /**
     * @return physical plan for query in string format.
     */
    KUZU_API std::string getPlan();
    void setPreparedSummary(PreparedSummary preparedSummary_);

    /**
     * @return true if the query is executed with EXPLAIN.
     */
    inline bool isExplain() const { return preparedSummary.isExplain; }

private:
    nlohmann::json& printPlanToJson();

private:
    double executionTime = 0;
    PreparedSummary preparedSummary;
    // Remove these two field once we have refactored the profiler using the existing pipeline
    // design.
    std::unique_ptr<nlohmann::json> planInJson;
    std::ostringstream planInOstream;
};

} // namespace main
} // namespace kuzu
