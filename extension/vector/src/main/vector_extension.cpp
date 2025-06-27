#include "main/vector_extension.h"

#include "catalog/hnsw_index_catalog_entry.h"
#include "function/hnsw_index_functions.h"
#include "main/client_context.h"
#include "main/database.h"
#include "storage/storage_manager.h"

namespace kuzu {
namespace vector_extension {

static void initHNSWEntries(main::ClientContext* context) {
    auto catalog = context->getCatalog();
    auto storageManager = context->getStorageManager();
    for (auto& indexEntry : catalog->getIndexEntries(context->getTransaction())) {
        if (indexEntry->getIndexType() == HNSWIndexCatalogEntry::TYPE_NAME &&
            !indexEntry->isLoaded()) {
            indexEntry->setAuxInfo(HNSWIndexAuxInfo::deserialize(indexEntry->getAuxBufferReader()));
            // Should load the index in storage side as well.
            auto& nodeTable =
                storageManager->getTable(indexEntry->getTableID())->cast<storage::NodeTable>();
            auto optionalIndex = nodeTable.getIndexHolder(indexEntry->getIndexName());
            KU_ASSERT_UNCONDITIONAL(
                optionalIndex.has_value() && !optionalIndex.value().get().isLoaded());
            auto& unloadedIndex = optionalIndex.value().get();
            unloadedIndex.load(context, storageManager);
        }
    }
}

void VectorExtension::load(main::ClientContext* context) {
    auto& db = *context->getDatabase();
    extension::ExtensionUtils::addTableFunc<QueryVectorIndexFunction>(context->getTransaction(),
        db);
    extension::ExtensionUtils::addInternalStandaloneTableFunc<InternalCreateHNSWIndexFunction>(
        context->getTransaction(), db);
    extension::ExtensionUtils::addInternalStandaloneTableFunc<InternalFinalizeHNSWIndexFunction>(
        context->getTransaction(), db);
    extension::ExtensionUtils::addStandaloneTableFunc<CreateVectorIndexFunction>(
        context->getTransaction(), db);
    extension::ExtensionUtils::addInternalStandaloneTableFunc<InternalDropHNSWIndexFunction>(
        context->getTransaction(), db);
    extension::ExtensionUtils::addStandaloneTableFunc<DropVectorIndexFunction>(
        context->getTransaction(), db);
    extension::ExtensionUtils::registerIndexType(db, OnDiskHNSWIndex::getIndexType());
    initHNSWEntries(context);
}

} // namespace vector_extension
} // namespace kuzu

#if defined(BUILD_DYNAMIC_LOAD)
extern "C" {
// Because we link against the static library on windows, we implicitly inherit KUZU_STATIC_DEFINE,
// which cancels out any exporting, so we can't use KUZU_API.
#if defined(_WIN32)
#define INIT_EXPORT __declspec(dllexport)
#else
#define INIT_EXPORT __attribute__((visibility("default")))
#endif
INIT_EXPORT void init(kuzu::main::ClientContext* context) {
    kuzu::vector_extension::VectorExtension::load(context);
}

INIT_EXPORT const char* name() {
    return kuzu::vector_extension::VectorExtension::EXTENSION_NAME;
}
}
#endif
