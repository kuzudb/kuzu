#pragma once

#include "plan_printer.h"
#include "query_result.h"
#include "query_summary.h"

namespace graphflow {
namespace main {

class PreparedStatement {
    friend class Connection;

public:
    PreparedStatement() {
        querySummary = make_unique<QuerySummary>();
        profiler = make_unique<Profiler>();
    }
    ~PreparedStatement() = default;

    inline bool isSuccess() const { return success; }
    inline string getErrorMessage() const { return errMsg; }

    inline void createResultHeader(vector<DataType> resultDataTypes) {
        resultHeader = make_unique<QueryResultHeader>(move(resultDataTypes));
    }

    inline void createPlanPrinter() {
        querySummary->planPrinter = make_unique<PlanPrinter>(
            move(physicalPlan), move(physicalIDToLogicalOperatorMap), move(profiler));
    }

private:
    bool success;
    string errMsg;

    unique_ptr<QuerySummary> querySummary;
    unique_ptr<Profiler> profiler;
    unordered_map<string, shared_ptr<Literal>> parameterMap;
    unique_ptr<QueryResultHeader> resultHeader;
    // TODO(Xiyang): Open an issue to remove this stupid map by copying whatever information needed
    // for plan printing.
    unordered_map<uint32_t, shared_ptr<LogicalOperator>> physicalIDToLogicalOperatorMap;
    // TODO(Xiyang): Open an issue about the life cycle of executionContext and profiler, maybe
    // using shared pointer is a more reasonable choice.
    unique_ptr<ExecutionContext> executionContext;
    unique_ptr<PhysicalPlan> physicalPlan;
};

} // namespace main
} // namespace graphflow
