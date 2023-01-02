#pragma once

#include "plan_printer.h"
#include "query_result.h"
#include "query_summary.h"

namespace kuzu {
namespace transaction {
class TinySnbDDLTest;
class TinySnbCopyCSVTransactionTest;
} // namespace transaction
} // namespace kuzu

namespace kuzu {
namespace main {

class PreparedStatement {
    friend class Connection;
    friend class JOConnection;
    friend class kuzu::transaction::TinySnbDDLTest;
    friend class kuzu::transaction::TinySnbCopyCSVTransactionTest;

public:
    inline bool isSuccess() const { return success; }
    inline string getErrorMessage() const { return errMsg; }

private:
    inline void createResultHeader(expression_vector expressions) {
        resultHeader = make_unique<QueryResultHeader>(std::move(expressions));
    }
    inline bool isReadOnly() { return logicalPlan->isReadOnly(); }

private:
    bool allowActiveTransaction;
    bool success = true;
    string errMsg;
    PreparedSummary preparedSummary;
    unordered_map<string, shared_ptr<Literal>> parameterMap;
    unique_ptr<QueryResultHeader> resultHeader;
    unique_ptr<LogicalPlan> logicalPlan;
};

} // namespace main
} // namespace kuzu
