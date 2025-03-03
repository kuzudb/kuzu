#include "vector_extension.h"

#include "catalog/hnsw_index_catalog_entry.h"
#include "function/hnsw_index_functions.h"
#include "main/client_context.h"
#include "main/database.h"

namespace kuzu {
namespace hnsw_extension {

static void initHNSWEntries(const transaction::Transaction* transaction,
    catalog::Catalog& catalog) {
    for (auto& indexEntry : catalog.getIndexEntries(transaction)) {
        if (indexEntry->getIndexType() == catalog::HNSWIndexCatalogEntry::TYPE_NAME) {
            indexEntry->setAuxInfo(
                catalog::HNSWIndexAuxInfo::deserialize(indexEntry->getAuxBufferReader()));
        }
    }
}

void VectorExtension::load(main::ClientContext* context) {
    auto& db = *context->getDatabase();
    extension::ExtensionUtils::addTableFunc<function::QueryHNSWIndexFunction>(db);
    extension::ExtensionUtils::addInternalStandaloneTableFunc<
        function::InternalCreateHNSWIndexFunction>(db);
    extension::ExtensionUtils::addInternalStandaloneTableFunc<
        function::InternalFinalizeHNSWIndexFunction>(db);
    extension::ExtensionUtils::addStandaloneTableFunc<function::CreateHNSWIndexFunction>(db);
    extension::ExtensionUtils::addInternalStandaloneTableFunc<
        function::InternalDropHNSWIndexFunction>(db);
    extension::ExtensionUtils::addStandaloneTableFunc<function::DropHNSWIndexFunction>(db);
    initHNSWEntries(context->getTransaction(), *db.getCatalog());
}

} // namespace hnsw_extension
} // namespace kuzu

extern "C" {
// Because we link against the static library on windows, we implicitly inherit KUZU_STATIC_DEFINE,
// which cancels out any exporting, so we can't use KUZU_API.
#if defined(_WIN32)
#define INIT_EXPORT __declspec(dllexport)
#else
#define INIT_EXPORT __attribute__((visibility("default")))
#endif
INIT_EXPORT void init(kuzu::main::ClientContext* context) {
    kuzu::hnsw_extension::VectorExtension::load(context);
}

INIT_EXPORT const char* name() {
    return kuzu::hnsw_extension::VectorExtension::EXTENSION_NAME;
}
}
