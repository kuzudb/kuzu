#pragma once

#include "main/client_context.h"

namespace kuzu::storage {
struct PageRange;
}
namespace kuzu {
namespace storage {

class Checkpointer {
public:
    explicit Checkpointer(main::ClientContext& clientContext);

    void writeCheckpoint();
    void rollback();

    void readCheckpoint();

    static bool canAutoCheckpoint(const main::ClientContext& clientContext);

private:
    void writeDatabaseHeader(const PageRange& catalogPageRange, const PageRange& metadataPageRange);

private:
    main::ClientContext& clientContext;
    bool isInMemory;
};

} // namespace storage
} // namespace kuzu
