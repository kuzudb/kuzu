#pragma once

#include "binder/bound_statement.h"
#include "bound_statement_result.h"
#include "common/enums/statement_type.h"

namespace kuzu {
namespace binder {

class BoundCommentOn : public BoundStatement {
public:
    BoundCommentOn(common::table_id_t tableID, std::string tableName, std::string comment)
        : BoundStatement{common::StatementType::COMMENT_ON,
              BoundStatementResult::createSingleStringColumnResult()},
          tableID(tableID), tableName(std::move(tableName)), comment(std::move(comment)) {}

    inline common::table_id_t getTableID() const { return tableID; }
    inline std::string getTableName() const { return tableName; }
    inline std::string getComment() const { return comment; }

private:
    common::table_id_t tableID;
    std::string tableName, comment;
};

} // namespace binder
} // namespace kuzu
