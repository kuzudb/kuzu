#pragma once

#include "binder/bound_statement.h"

namespace kuzu {
namespace binder {

class BoundDropTable final : public BoundStatement {
public:
    BoundDropTable(common::table_id_t tableID, std::string tableName)
        : BoundStatement{common::StatementType::DROP_TABLE,
              BoundStatementResult::createSingleStringColumnResult()},
          tableID{tableID}, tableName{std::move(tableName)} {}

    inline common::table_id_t getTableID() const { return tableID; }
    inline std::string getTableName() const { return tableName; }

private:
    common::table_id_t tableID;
    std::string tableName;
};

} // namespace binder
} // namespace kuzu
