#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "binder/bound_statement.h"
#include "catalog/table_schema.h"
#include "common/copier_config/copier_config.h"

namespace kuzu {
namespace binder {

class BoundCopyFrom : public BoundStatement {
public:
    BoundCopyFrom(
        common::CopyDescription copyDescription, common::table_id_t tableID, std::string tableName)
        : BoundStatement{common::StatementType::COPY_FROM,
              BoundStatementResult::createSingleStringColumnResult()},
          copyDescription{copyDescription}, tableID{tableID}, tableName{std::move(tableName)} {}

    inline common::CopyDescription getCopyDescription() const { return copyDescription; }

    inline common::table_id_t getTableID() const { return tableID; }

    inline std::string getTableName() const { return tableName; }

private:
    common::CopyDescription copyDescription;
    common::table_id_t tableID;
    std::string tableName;
};

} // namespace binder
} // namespace kuzu
