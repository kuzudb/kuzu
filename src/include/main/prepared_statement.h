#pragma once

#include "query_summary.h"

namespace kuzu::testing {
class TestHelper;
}

namespace kuzu::transaction {
class TinySnbDDLTest;
class TinySnbCopyCSVTransactionTest;
} // namespace kuzu::transaction

namespace kuzu {
namespace main {

class PreparedStatement {
    friend class Connection;
    friend class JOConnection;
    friend class kuzu::testing::TestHelper;
    friend class kuzu::transaction::TinySnbDDLTest;
    friend class kuzu::transaction::TinySnbCopyCSVTransactionTest;

public:
    inline bool isSuccess() const { return success; }
    inline string getErrorMessage() const { return errMsg; }
    inline bool isReadOnly() const { return readOnly; }
    inline expression_vector getExpressionsToCollect() {
        return statementResult->getExpressionsToCollect();
    }

private:
    StatementType statementType;
    bool allowActiveTransaction = false;
    bool success = true;
    bool readOnly = false;
    string errMsg;
    PreparedSummary preparedSummary;
    unordered_map<string, shared_ptr<Literal>> parameterMap;
    unique_ptr<BoundStatementResult> statementResult;
    vector<unique_ptr<LogicalPlan>> logicalPlans;
};

} // namespace main
} // namespace kuzu
