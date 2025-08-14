#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/api.h"
#include "common/types/value/value.h"
#include "query_summary.h"

namespace kuzu {
namespace common {
class LogicalType;
}
namespace parser {
class Statement;
}
namespace binder {
class Expression;
}
namespace planner {
class LogicalPlan;
}

namespace main {

// Prepared statement cached in client context and NEVER serialized to client side.
struct CachedPreparedStatement {
    bool useInternalCatalogEntry = false;
    std::shared_ptr<parser::Statement> parsedStatement;
    std::unique_ptr<planner::LogicalPlan> logicalPlan;
    std::vector<std::shared_ptr<binder::Expression>> columns;

    CachedPreparedStatement();
    ~CachedPreparedStatement();

    std::vector<std::string> getColumnNames() const;
    std::vector<common::LogicalType> getColumnTypes() const;
};

/**
 * @brief A prepared statement is a parameterized query which can avoid planning the same query for
 * repeated execution.
 */
class PreparedStatement {
    friend class Connection;
    friend class ClientContext;

public:
    KUZU_API ~PreparedStatement();
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

    std::unordered_map<std::string, std::shared_ptr<common::Value>>& getParameterMapUnsafe() {
        return parameterMap;
    }

    std::string getName() const { return cachedPreparedStatementName; }

    common::StatementType getStatementType() const;

    void validateExecuteParam(const std::string& paramName, common::Value* param) const;

    static std::unique_ptr<PreparedStatement> getPreparedStatementWithError(
        const std::string& errorMessage);

private:
    bool success = true;
    bool readOnly = true;
    std::string errMsg;
    PreparedSummary preparedSummary;
    std::string cachedPreparedStatementName;
    std::unordered_map<std::string, std::shared_ptr<common::Value>> parameterMap;
};

} // namespace main
} // namespace kuzu
