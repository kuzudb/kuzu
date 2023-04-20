#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "binder/bound_statement.h"
#include "catalog/catalog_structs.h"
#include "common/copier_config/copier_config.h"

namespace kuzu {
namespace binder {

class BoundCopy : public BoundStatement {
public:
    BoundCopy(
        common::CopyDescription copyDescription, common::table_id_t tableID, std::string tableName)
        : BoundStatement{common::StatementType::COPY,
              BoundStatementResult::createSingleStringColumnResult()},
          copyDescription{std::move(copyDescription)}, tableID{tableID}, tableName{std::move(
                                                                             tableName)} {}

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
