#pragma once

#include "binder/bound_statement.h"
#include "parser/statement.h"

namespace kuzu {
namespace extension {

class KUZU_API BinderExtension {
public:
    BinderExtension() {}

    virtual ~BinderExtension() = default;

    virtual std::unique_ptr<binder::BoundStatement> bind(const parser::Statement& statement) = 0;
};

} // namespace extension
} // namespace kuzu
