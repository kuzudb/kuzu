#pragma once

#include "binder/bound_statement.h"
#include "parser/ddl/drop_info.h"

namespace kuzu {
namespace binder {

class BoundDrop final : public BoundStatement {
public:
    explicit BoundDrop(const parser::DropInfo& dropInfo)
        : BoundStatement{common::StatementType::DROP,
              BoundStatementResult::createSingleStringColumnResult()},
          dropInfo{std::move(dropInfo)} {}

    const parser::DropInfo& getDropInfo() const { return dropInfo; };

private:
    parser::DropInfo dropInfo;
};

} // namespace binder
} // namespace kuzu
