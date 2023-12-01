#pragma once

#include "alter_info.h"
#include "parser/statement.h"

namespace kuzu {
namespace parser {

class Alter : public Statement {
public:
    explicit Alter(std::unique_ptr<AlterInfo> info)
        : Statement{common::StatementType::ALTER}, info{std::move(info)} {}

    inline AlterInfo* getInfo() const { return info.get(); }

private:
    std::unique_ptr<AlterInfo> info;
};

} // namespace parser
} // namespace kuzu
