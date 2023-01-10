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

class BoundCopyCSV : public BoundStatement {
public:
    BoundCopyCSV(CSVDescription csvDescription, table_id_t tableID, string tableName)
        : BoundStatement{StatementType::COPY_CSV,
              BoundStatementResult::createSingleStringColumnResult()},
          csvDescription{std::move(csvDescription)}, tableID{tableID}, tableName{
                                                                           std::move(tableName)} {}

    inline CSVDescription getCSVDescription() const { return csvDescription; }

    inline table_id_t getTableID() const { return tableID; }

    inline string getTableName() const { return tableName; }

private:
    CSVDescription csvDescription;
    table_id_t tableID;
    string tableName;
};

} // namespace binder
} // namespace kuzu
