#include "binder/binder.h"
#include "binder/bound_detach_database.h"
#include "common/cast.h"
#include "parser/detach_database.h"

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindDetachDatabase(const parser::Statement& statement) {
    auto& detachDatabase =
        common::ku_dynamic_cast<const parser::Statement&, const parser::DetachDatabase&>(statement);
    return std::make_unique<BoundDetachDatabase>(detachDatabase.getDBName());
}

} // namespace binder
} // namespace kuzu
