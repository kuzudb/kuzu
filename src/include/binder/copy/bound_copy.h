#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "binder/bound_statement.h"
#include "catalog/catalog_structs.h"
#include "common/csv_reader/csv_reader.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

class BoundCopy : public BoundStatement {
public:
    BoundCopy(CopyDescription copyDescription, table_id_t tableID, string tableName)
        : BoundStatement{StatementType::COPY_CSV,
              BoundStatementResult::createSingleStringColumnResult()},
          copyDescription{std::move(copyDescription)}, tableID{tableID}, tableName{std::move(
                                                                             tableName)} {}

    inline CopyDescription getCopyDescription() const { return copyDescription; }

    inline table_id_t getTableID() const { return tableID; }

    inline string getTableName() const { return tableName; }

private:
    CopyDescription copyDescription;
    table_id_t tableID;
    string tableName;
};

} // namespace binder
} // namespace kuzu
