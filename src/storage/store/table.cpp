#include "storage/store/table.h"

using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {
namespace storage {

Table::Table(TableSchema* tableSchema, TablesStatistics* tablesStatistics,
    BufferManager& bufferManager, WAL* wal)
    : tableType{tableSchema->tableType}, tablesStatistics{tablesStatistics},
      tableID{tableSchema->tableID}, bufferManager{bufferManager}, wal{wal} {}

} // namespace storage
} // namespace kuzu
