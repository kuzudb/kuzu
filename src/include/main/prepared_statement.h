#pragma once
#include "../common/statement_type.h"
#include "../common/types/value.h"
#include "forward_declarations.h"
#include "query_summary.h"

namespace kuzu {
namespace main {

using namespace kuzu::planner;

class PreparedStatement {
    friend class Connection;
    friend class JOConnection;
    friend class kuzu::testing::TestHelper;
    friend class kuzu::transaction::TinySnbDDLTest;
    friend class kuzu::transaction::TinySnbCopyCSVTransactionTest;

public:
    inline bool allowActiveTransaction() const {
        return !StatementTypeUtils::isDDLOrCopyCSV(statementType);
    }

    inline bool isSuccess() const { return success; }

    inline string getErrorMessage() const { return errMsg; }

    inline bool isReadOnly() const { return readOnly; }

    expression_vector getExpressionsToCollect();

private:
    StatementType statementType;
    bool success = true;
    bool readOnly = false;
    string errMsg;
    PreparedSummary preparedSummary;
    unordered_map<string, shared_ptr<Value>> parameterMap;
    unique_ptr<binder::BoundStatementResult> statementResult;
    vector<unique_ptr<LogicalPlan>> logicalPlans;
};

} // namespace main
} // namespace kuzu
