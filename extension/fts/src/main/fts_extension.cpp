#include "main/fts_extension.h"

#include "catalog/catalog.h"
#include "catalog/fts_index_catalog_entry.h"
#include "function/create_fts_index.h"
#include "function/drop_fts_index.h"
#include "function/query_fts_index.h"
#include "function/stem.h"
#include "index/fts_index.h"
#include "main/client_context.h"
#include "storage/storage_manager.h"

namespace kuzu {
namespace fts_extension {

using namespace extension;

static void initFTSEntries(main::ClientContext* context, catalog::Catalog& catalog) {
    auto storageManager = context->getStorageManager();
    for (auto& indexEntry : catalog.getIndexEntries(context->getTransaction())) {
        if (indexEntry->getIndexType() == FTSIndexCatalogEntry::TYPE_NAME &&
            !indexEntry->isLoaded()) {
            indexEntry->setAuxInfo(FTSIndexAuxInfo::deserialize(indexEntry->getAuxBufferReader()));
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

void FtsExtension::load(main::ClientContext* context) {
    auto& db = *context->getDatabase();
    ExtensionUtils::addScalarFunc<StemFunction>(db);
    ExtensionUtils::addTableFunc<QueryFTSFunction>(db);
    ExtensionUtils::addStandaloneTableFunc<CreateFTSFunction>(db);
    ExtensionUtils::addInternalStandaloneTableFunc<InternalCreateFTSFunction>(db);
    ExtensionUtils::addStandaloneTableFunc<DropFTSFunction>(db);
    ExtensionUtils::addInternalStandaloneTableFunc<InternalDropFTSFunction>(db);
    ExtensionUtils::registerIndexType(db, FTSIndex::getIndexType());
    initFTSEntries(context, *db.getCatalog());
}

} // namespace fts_extension
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
    kuzu::fts_extension::FtsExtension::load(context);
}

INIT_EXPORT const char* name() {
    return kuzu::fts_extension::FtsExtension::EXTENSION_NAME;
}
}
#endif
