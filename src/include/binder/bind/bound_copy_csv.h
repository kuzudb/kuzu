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
    BoundCopyCSV(CSVDescription csvDescription, TableSchema tableSchema)
        : BoundStatement{StatementType::COPY_CSV}, csvDescription{move(csvDescription)},
          tableSchema{move(tableSchema)} {}

    inline CSVDescription getCSVDescription() const { return csvDescription; }

    inline TableSchema getTableSchema() const { return tableSchema; }

private:
    CSVDescription csvDescription;
    TableSchema tableSchema;
};

} // namespace binder
} // namespace kuzu
