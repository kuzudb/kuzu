#pragma once

#include "binder/bound_statement.h"
#include "bound_alter_info.h"

namespace kuzu {
namespace binder {

class BoundAlter : public BoundStatement {
public:
    BoundAlter(std::unique_ptr<BoundAlterInfo> info)
        : BoundStatement{common::StatementType::ALTER,
              BoundStatementResult::createSingleStringColumnResult()},
          info{std::move(info)} {}

    inline BoundAlterInfo* getInfo() const { return info.get(); }

private:
    std::unique_ptr<BoundAlterInfo> info;
};

} // namespace binder
} // namespace kuzu
