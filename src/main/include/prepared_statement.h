#pragma once

#include "plan_printer.h"
#include "query_result.h"
#include "query_summary.h"

namespace graphflow {
namespace transaction {
class TinySnbDDLTest;
class TinySnbCopyCSVTransactionTest;
} // namespace transaction
} // namespace graphflow

namespace graphflow {
namespace main {

class PreparedStatement {
    friend class Connection;
    friend class JOConnection;
    friend class graphflow::transaction::TinySnbDDLTest;
    friend class graphflow::transaction::TinySnbCopyCSVTransactionTest;

public:
    inline bool isSuccess() const { return success; }
    inline string getErrorMessage() const { return errMsg; }

    inline void createResultHeader(expression_vector expressions) {
        resultHeader = make_unique<QueryResultHeader>(move(expressions));
    }

    inline bool isReadOnly() { return physicalPlan->isReadOnly(); }

private:
    bool allowActiveTransaction;
    bool success = true;
    string errMsg;
    PreparedSummary preparedSummary;
    unordered_map<string, shared_ptr<Literal>> parameterMap;
    unique_ptr<QueryResultHeader> resultHeader;
    unique_ptr<PhysicalPlan> physicalPlan;
};

} // namespace main
} // namespace graphflow
