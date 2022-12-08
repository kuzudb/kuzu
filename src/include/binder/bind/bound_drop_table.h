#pragma once

#include "binder/bound_statement.h"

namespace kuzu {
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
} // namespace kuzu
