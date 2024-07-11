#pragma once

#include "drop_info.h"
#include "parser/statement.h"

namespace kuzu {
namespace parser {

class Drop : public Statement {
public:
    explicit Drop(DropInfo dropInfo)
        : Statement{common::StatementType::DROP}, dropInfo{std::move(dropInfo)} {}

    const DropInfo& getDropInfo() const { return dropInfo; }

private:
    DropInfo dropInfo;
};

} // namespace parser
} // namespace kuzu
