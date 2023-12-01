#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/api.h"
#include "kuzu_fwd.h"
#include "query_summary.h"

namespace kuzu {
namespace main {

/**
 * @brief A prepared statement is a parameterized query which can avoid planning the same query for
 * repeated execution.
 */
class PreparedStatement {
    friend class Connection;
    friend class testing::TestHelper;
    friend class testing::TestRunner;
    friend class testing::TinySnbDDLTest;
    friend class testing::TinySnbCopyCSVTransactionTest;

public:
    /**
     * @brief DDL and COPY statements are automatically wrapped in a transaction and committed.
     * As such, they cannot be part of an active transaction.
     * @return the prepared statement is allowed to be part of an active transaction.
     */
    KUZU_API bool allowActiveTransaction() const;
    bool isTransactionStatement() const;
    /**
     * @return the query is prepared successfully or not.
     */
    KUZU_API bool isSuccess() const;
    /**
     * @return the error message if the query is not prepared successfully.
     */
    KUZU_API std::string getErrorMessage() const;
    /**
     * @return the prepared statement is read-only or not.
     */
    KUZU_API bool isReadOnly() const;

    inline std::unordered_map<std::string, std::shared_ptr<common::Value>> getParameterMap() {
        return parameterMap;
    }

    KUZU_API ~PreparedStatement();

private:
    bool isProfile();

private:
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
