#include "catalog/catalog_entry/index_catalog_entry.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "main/client_context.h"
#include "storage/table/node_table.h"

namespace kuzu {
namespace fts_extension {

class FTSUpdater {
    FTSUpdater();

    void insertNode(main::ClientContext* context, common::nodeID_t insertedNodeID,
        std::vector<common::ValueVector*> columnDataVectors);

    void deleteNode(transaction::Transaction* transaction, common::nodeID_t deletedNodeID);

private:
    catalog::IndexCatalogEntry* ftsEntry;
    std::vector<common::idx_t> dataVectorIdxes;
    // Doc table
    storage::NodeTable* docTable;
    common::ValueVector docNodeIDVector;
    common::ValueVector docPKVector;
    common::ValueVector lenVector;
    std::vector<common::ValueVector*> docProperties;
    // Dict table
    storage::NodeTable* dictTable;
    common::ValueVector dictNodeIDVector;
    common::ValueVector dictPKVector;
    common::ValueVector dictDFVector;
    std::vector<common::ValueVector*> dictProperties;
};

} // namespace fts_extension
} // namespace kuzu
