#include "binder/binder.h"
#include "binder/bound_update_vector_index.h"
#include "catalog/catalog.h"
#include "common/exception/binder.h"
#include "parser/update_vector_index.h"
#include "main/client_context.h"

using namespace kuzu::common;
using namespace kuzu::parser;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindUpdateVectorIndex(const parser::Statement& statement) {
    auto& updateVectorIndex =
        ku_dynamic_cast<const Statement&, const UpdateVectorIndex&>(statement);
    auto catalog = clientContext->getCatalog();
    auto tableEntry = catalog->getTableCatalogEntry(clientContext->getTx(),
        bindTableID(updateVectorIndex.getTableName()));

    if (tableEntry->getTableType() != TableType::NODE) {
        throw BinderException("Vector index can only be created on node tables.");
    }
    auto propertyId = tableEntry->getPropertyID(updateVectorIndex.getPropertyName());
    return std::make_unique<BoundUpdateVectorIndex>(tableEntry, propertyId);
}

} // namespace binder
} // namespace kuzu
