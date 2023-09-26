#pragma once

#include "binder/bound_statement.h"
#include "bound_create_table_info.h"

namespace kuzu {
namespace binder {

class BoundCreateTable : public BoundStatement {
public:
    explicit BoundCreateTable(std::unique_ptr<BoundCreateTableInfo> info)
        : BoundStatement{common::StatementType::CREATE_TABLE,
              BoundStatementResult::createSingleStringColumnResult()},
          info{std::move(info)} {}

    inline BoundCreateTableInfo* getInfo() const { return info.get(); }

private:
    std::unique_ptr<BoundCreateTableInfo> info;
};

} // namespace binder
} // namespace kuzu
