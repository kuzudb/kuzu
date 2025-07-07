#pragma once

#include "storage/optimistic_allocator.h"
#include "storage/page_range.h"

namespace kuzu {
namespace catalog {
class Catalog;
}
namespace common {
class VirtualFileSystem;
}
namespace testing {
struct FSMLeakChecker;
}
namespace main {
class AttachedKuzuDatabase;
} // namespace main

namespace storage {
class StorageManager;

class PageManager;

struct DatabaseHeader {
    PageRange catalogPageRange;
    PageRange metadataPageRange;

    void updateCatalogPageRange(PageManager& pageManager, PageRange newPageRange);
    void freeMetadataPageRange(PageManager& pageManager) const;
    void serialize(common::Serializer& ser) const;
};

class Checkpointer {
    friend class main::AttachedKuzuDatabase;
    friend struct testing::FSMLeakChecker;

public:
    explicit Checkpointer(main::ClientContext& clientContext);
    virtual ~Checkpointer();

    void writeCheckpoint();
    void rollback();

    void readCheckpoint();

    static bool canAutoCheckpoint(const main::ClientContext& clientContext);

protected:
    virtual bool checkpointStorage();
    virtual void serializeCatalogAndMetadata(DatabaseHeader& databaseHeader,
        bool hasStorageChanges);
    virtual void writeDatabaseHeader(const DatabaseHeader& header);
    virtual void logCheckpointAndApplyShadowPages();

private:
    static void readCheckpoint(const std::string& dbPath, main::ClientContext* context,
        common::VirtualFileSystem* vfs, catalog::Catalog* catalog, StorageManager* storageManager);

    DatabaseHeader getCurrentDatabaseHeader() const;
    PageRange serializeCatalog(const catalog::Catalog& catalog, StorageManager& storageManager);
    PageRange serializeMetadata(const catalog::Catalog& catalog, StorageManager& storageManager);

protected:
    main::ClientContext& clientContext;
    bool isInMemory;
    OptimisticAllocator pageAllocator;
};

} // namespace storage
} // namespace kuzu
