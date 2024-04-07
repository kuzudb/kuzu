#include "binder/binder.h"
#include "binder/bound_attach_database.h"
#include "common/cast.h"
#include "parser/attach_database.h"

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindAttachDatabase(const parser::Statement& statement) {
    auto& attachDatabase =
        common::ku_dynamic_cast<const parser::Statement&, const parser::AttachDatabase&>(statement);
    return std::make_unique<BoundAttachDatabase>(attachDatabase.getAttachInfo());
}

} // namespace binder
} // namespace kuzu
