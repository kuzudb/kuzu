#pragma once

#include "catalog/catalog_entry/table_catalog_entry.h"
#include "main/client_context.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/table/node_table.h"

namespace kuzu {
namespace fts_extension {

class TermsDFLookup {
public:
    static constexpr char DOC_FREQUENCY_PROP_NAME[] = "df";

public:
    TermsDFLookup(catalog::TableCatalogEntry* termsEntry, main::ClientContext& context);

    std::pair<common::offset_t, uint64_t> lookupTermDF(const std::string& term);

private:
    std::shared_ptr<common::DataChunkState> dataChunkState;
    common::ValueVector termsVector;
    common::ValueVector nodeIDVector;
    common::ValueVector dfVector;
    storage::NodeTable& termsTable;
    storage::NodeTableScanState nodeTableScanState;
    common::column_id_t dfColumnID;
    transaction::Transaction* trx;
};

} // namespace fts_extension
} // namespace kuzu
