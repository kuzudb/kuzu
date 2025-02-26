#pragma once

#include "storage/store/column_chunk_data.h"
#include "transaction/transaction.h"

namespace kuzu {
namespace storage {

class CachedColumn final : public transaction::LocalCacheObject {
public:
    static std::string getKey(common::table_id_t tableID, common::property_id_t propertyID) {
        return common::stringFormat("{}-{}", tableID, propertyID);
    }
    explicit CachedColumn(common::table_id_t tableID, common::property_id_t propertyID)
        : LocalCacheObject{getKey(tableID, propertyID)}, columnChunks{} {}

    std::vector<std::unique_ptr<ColumnChunkData>> columnChunks;
};

} // namespace storage
} // namespace kuzu
