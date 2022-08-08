#pragma once

#include "plan_printer.h"
#include "query_result.h"
#include "query_summary.h"

namespace graphflow {
namespace transaction {
class TinySnbDDLTest;
class TinySnbCopyCSVTest;
} // namespace transaction
} // namespace graphflow

namespace graphflow {
namespace main {

class PreparedStatement {
    friend class Connection;
    friend class JOConnection;
    friend class graphflow::transaction::TinySnbDDLTest;
    friend class graphflow::transaction::TinySnbCopyCSVTest;

public:
    PreparedStatement() { querySummary = make_unique<QuerySummary>(); }
    ~PreparedStatement() = default;

    inline bool isSuccess() const { return success; }
    inline string getErrorMessage() const { return errMsg; }

    inline void createResultHeader(expression_vector expressions) {
        resultHeader = make_unique<QueryResultHeader>(move(expressions));
    }

    inline void createPlanPrinter(unique_ptr<Profiler> profiler) {
        querySummary->planPrinter = make_unique<PlanPrinter>(move(physicalPlan), move(profiler));
    }

    inline bool isReadOnly() { return physicalPlan->isReadOnly(); }

private:
    bool isDDL;
    bool success = true;
    string errMsg;

    unique_ptr<QuerySummary> querySummary;
    unordered_map<string, shared_ptr<Literal>> parameterMap;
    unique_ptr<QueryResultHeader> resultHeader;
    unique_ptr<PhysicalPlan> physicalPlan;
};

} // namespace main
} // namespace graphflow
