#pragma once

#include "alter_info.h"
#include "parser/statement.h"

namespace kuzu {
namespace parser {

class Alter : public Statement {
public:
    explicit Alter(AlterInfo info)
        : Statement{common::StatementType::ALTER}, info{std::move(info)} {}

    inline const AlterInfo* getInfo() const { return &info; }

private:
    AlterInfo info;
};

} // namespace parser
} // namespace kuzu
