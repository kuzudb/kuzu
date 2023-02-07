#pragma once

#include "common/api.h"
#include "kuzu_fwd.h"
#include "query_summary.h"

namespace kuzu {
namespace main {

class PreparedStatement {
    friend class Connection;
    friend class JOConnection;
    friend class kuzu::testing::TestHelper;
    friend class kuzu::transaction::TinySnbDDLTest;
    friend class kuzu::transaction::TinySnbCopyCSVTransactionTest;

public:
    KUZU_API bool allowActiveTransaction() const;
    KUZU_API bool isSuccess() const;
    KUZU_API std::string getErrorMessage() const;
    KUZU_API bool isReadOnly() const;
    std::vector<std::shared_ptr<binder::Expression>> getExpressionsToCollect();

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
