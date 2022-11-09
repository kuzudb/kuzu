#pragma once

#include "src/binder/include/bound_statement.h"

namespace graphflow {
namespace binder {

class BoundDropTable : public BoundStatement {
public:
    explicit BoundDropTable(TableSchema* tableSchema)
        : BoundStatement{StatementType::DROP_TABLE}, tableSchema{tableSchema} {}

    inline TableSchema* getTableSchema() const { return tableSchema; }

private:
    TableSchema* tableSchema;
};

} // namespace binder
} // namespace graphflow
