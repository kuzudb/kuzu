#include "storage/store/rels_store.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

RelsStore::RelsStore(BMFileHandle* dataFH, BMFileHandle* metadataFH,
    const catalog::Catalog& catalog, BufferManager& bufferManager, WAL* wal, bool enableCompression)
    : wal{wal}, dataFH{dataFH}, metadataFH{metadataFH}, enableCompression{enableCompression} {
    relsStatistics = std::make_unique<RelsStoreStats>(metadataFH, &bufferManager, wal);
    for (auto schema : catalog.getReadOnlyVersion()->getRelTableSchemas()) {
        auto relTableSchema = dynamic_cast<RelTableSchema*>(schema);
        relTables.emplace(
            schema->tableID, std::make_unique<RelTable>(dataFH, metadataFH, relsStatistics.get(),
                                 &bufferManager, relTableSchema, wal, enableCompression));
    }
}

} // namespace storage
} // namespace kuzu
