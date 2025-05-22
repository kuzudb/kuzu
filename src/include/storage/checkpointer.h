#pragma once

#include "main/client_context.h"

namespace kuzu {
namespace main {
class AttachedKuzuDatabase;
} // namespace main

namespace storage {

struct PageRange;

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
    void writeDatabaseHeader(const PageRange& catalogPageRange, const PageRange& metadataPageRange);

private:
    main::ClientContext& clientContext;
    bool isInMemory;
};

} // namespace storage
} // namespace kuzu
