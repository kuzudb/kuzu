#pragma once

#include "main/client_context.h"
#include "storage/page_range.h"

namespace kuzu {
namespace main {
class AttachedKuzuDatabase;
} // namespace main

namespace storage {

class PageManager;

struct DatabaseHeader {
    PageRange catalogPageRange;
    PageRange metadataPageRange;

    void updateCatalogPageRange(PageManager& pageManager, PageRange newPageRange);
    void updateMetadataPageRange(PageManager& pageManager, PageRange newPageRange);
};

class Checkpointer {
    friend class main::AttachedKuzuDatabase;

public:
    explicit Checkpointer(main::ClientContext& clientContext);

    void writeCheckpoint();
    void rollback();

    void readCheckpoint();

    static bool canAutoCheckpoint(const main::ClientContext& clientContext);

private:
    static void readCheckpoint(const std::string& dbPath, main::ClientContext* context,
        common::VirtualFileSystem* vfs, catalog::Catalog* catalog, StorageManager* storageManager);

private:
    void writeDatabaseHeader(const DatabaseHeader& header);
    DatabaseHeader getCurrentDatabaseHeader() const;
    PageRange serializeCatalog(catalog::Catalog& catalog, StorageManager& storageManager);
    PageRange serializeMetadata(catalog::Catalog& catalog, StorageManager& storageManager);

private:
    main::ClientContext& clientContext;
    bool isInMemory;
};

} // namespace storage
} // namespace kuzu
