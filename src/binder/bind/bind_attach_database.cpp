#include "binder/binder.h"
#include "binder/bound_attach_database.h"
#include "parser/attach_database.h"

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindAttachDatabase(const parser::Statement& statement) {
    auto& attachDatabase = statement.constCast<parser::AttachDatabase>();
    return std::make_unique<BoundAttachDatabase>(attachDatabase.getAttachInfo());
}

} // namespace binder
} // namespace kuzu
