#pragma once

#include "storage/store/table_data.h"

namespace kuzu {
namespace storage {

class NodeTableData : public TableData {
public:
    NodeTableData(BMFileHandle* dataFH, BMFileHandle* metadataFH, common::table_id_t tableID,
        BufferManager* bufferManager, WAL* wal, const std::vector<catalog::Property*>& properties,
        TablesStatistics* tablesStatistics, bool enableCompression);
};

} // namespace storage
} // namespace kuzu
