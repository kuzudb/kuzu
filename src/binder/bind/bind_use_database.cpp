#include "binder/binder.h"
#include "binder/bound_use_database.h"
#include "common/cast.h"
#include "parser/use_database.h"

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindUseDatabase(const parser::Statement& statement) {
    auto& useDatabase =
        common::ku_dynamic_cast<const parser::Statement&, const parser::UseDatabase&>(statement);
    return std::make_unique<BoundUseDatabase>(useDatabase.getDBName());
}

} // namespace binder
} // namespace kuzu
