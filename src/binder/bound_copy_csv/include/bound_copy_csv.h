#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "src/binder/include/bound_statement.h"
#include "src/catalog/include/catalog_structs.h"
#include "src/common/include/csv_reader/csv_reader.h"

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
