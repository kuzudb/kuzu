#pragma once

#include "binder/bound_statement.h"
#include "bound_create_table_info.h"

namespace kuzu {
namespace binder {

class BoundCreateTable final : public BoundStatement {
public:
    explicit BoundCreateTable(BoundCreateTableInfo info)
        : BoundStatement{common::StatementType::CREATE_TABLE,
              BoundStatementResult::createSingleStringColumnResult()},
          info{std::move(info)} {}
    
    BoundCreateTable(BoundCreateTableInfo info, BoundCopyFromInfo copyInfo)
        : BoundStatement{common::StatementType::CREATE_TABLE,
              BoundStatementResult::createSingleStringColumnResult()},
          info{std::move(info)}, copyInfo{std::move(copyInfo)} {}

    inline const BoundCreateTableInfo* getInfo() const { return &info; }
    inline bool hasCopyInfo() const { return copyInfo.has_value(); }
    const BoundCopyFromInfo* getCopyInfo() const { return &copyInfo.value(); }

private:
    BoundCreateTableInfo info;
    std::optional<BoundCopyFromInfo> copyInfo;
};

} // namespace binder
} // namespace kuzu
