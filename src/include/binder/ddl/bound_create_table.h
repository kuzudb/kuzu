#pragma once

#include "binder/bound_statement.h"
#include "bound_create_table_info.h"

namespace kuzu {
namespace binder {

class BoundCreateTable final : public BoundStatement {
    static constexpr common::StatementType type_ = common::StatementType::CREATE_TABLE;

public:
    explicit BoundCreateTable(BoundCreateTableInfo info)
        : BoundStatement{type_, BoundStatementResult::createSingleStringColumnResult()},
          info{std::move(info)} {}
    
    BoundCreateTable(BoundCreateTableInfo info, BoundCopyFromInfo copyInfo)
        : BoundStatement{common::StatementType::CREATE_TABLE,
              BoundStatementResult::createSingleStringColumnResult()},
          info{std::move(info)}, copyInfo{std::move(copyInfo)} {}

    const BoundCreateTableInfo& getInfo() const { return info; }

private:
    BoundCreateTableInfo info;
    std::optional<BoundCopyFromInfo> copyInfo;
};

} // namespace binder
} // namespace kuzu
