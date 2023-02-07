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
    inline bool allowActiveTransaction() const {
        return !common::StatementTypeUtils::isDDLOrCopyCSV(statementType);
    }

    inline bool isSuccess() const { return success; }

    inline std::string getErrorMessage() const { return errMsg; }

    inline bool isReadOnly() const { return readOnly; }

    inline binder::expression_vector getExpressionsToCollect() {
        return statementResult->getExpressionsToCollect();
    }

private:
    common::StatementType statementType;
    bool success = true;
    bool readOnly = false;
    std::string errMsg;
    PreparedSummary preparedSummary;
    std::unordered_map<std::string, std::shared_ptr<common::Value>> parameterMap;
    std::unique_ptr<binder::BoundStatementResult> statementResult;
    std::vector<std::unique_ptr<planner::LogicalPlan>> logicalPlans;
};

} // namespace main
} // namespace kuzu
