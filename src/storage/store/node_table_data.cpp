#include "storage/store/node_table_data.h"

#include "storage/stats/nodes_store_statistics.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

NodeTableData::NodeTableData(BMFileHandle* dataFH, BMFileHandle* metadataFH, table_id_t tableID,
    BufferManager* bufferManager, WAL* wal, const std::vector<Property*>& properties,
    TablesStatistics* tablesStatistics, bool enableCompression)
    : TableData{dataFH, metadataFH, tableID, bufferManager, wal, enableCompression,
          ColumnDataFormat::REGULAR_COL} {
    columns.reserve(properties.size());
    for (auto i = 0u; i < properties.size(); i++) {
        auto property = properties[i];
        auto metadataDAHInfo =
            dynamic_cast<NodesStoreStatsAndDeletedIDs*>(tablesStatistics)
                ->getMetadataDAHInfo(Transaction::getDummyWriteTrx().get(), tableID, i);
        columns.push_back(ColumnFactory::createColumn(*property->getDataType(), *metadataDAHInfo,
            dataFH, metadataFH, bufferManager, wal, Transaction::getDummyReadOnlyTrx().get(),
            RWPropertyStats(tablesStatistics, tableID, property->getPropertyID()),
            enableCompression));
    }
}

} // namespace storage
} // namespace kuzu
