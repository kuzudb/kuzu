#pragma once

#include "storage/index/index.h"
#include "storage/table/node_table.h"
#include "storage/table/rel_table.h"

namespace kuzu {
namespace fts_extension {

struct FTSInternalTableInfo {
    storage::NodeTable* stopWordsTable;
    storage::NodeTable* docTable;
    storage::NodeTable* termsTable;
    storage::RelTable* appearsInfoTable;
    common::column_id_t dfColumnID;

    FTSInternalTableInfo(main::ClientContext* context, common::table_id_t tableID,
        const std::string& indexName, const std::string& stopWordsTableName);
};

} // namespace fts_extension
} // namespace kuzu
