#include "catalog/catalog_entry/index_catalog_entry.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "main/client_context.h"
#include "storage/table/node_table.h"
#include "storage/table/rel_table.h"

namespace kuzu {
namespace fts_extension {

class FTSUpdater {
public:
    FTSUpdater(catalog::IndexCatalogEntry* ftsEntry, main::ClientContext* context);

    void insertNode(main::ClientContext* context, common::nodeID_t insertedNodeID,
        std::vector<common::ValueVector*> columnDataVectors);

    void deleteNode(main::ClientContext* context, common::nodeID_t deletedNodeID);

private:
    catalog::IndexCatalogEntry* ftsEntry;
    std::vector<common::idx_t> dataVectorIdxes = {1};
    std::shared_ptr<common::DataChunkState> dataChunkState;
    // Doc table
    storage::NodeTable* docTable;
    common::ValueVector docNodeIDVector;
    common::ValueVector docPKVector;
    common::ValueVector lenVector;
    std::vector<common::ValueVector*> docProperties;
    // Terms table
    storage::NodeTable* termsTable;
    common::ValueVector termsNodeIDVector;
    common::ValueVector termsPKVector;
    common::ValueVector termsDFVector;
    std::vector<common::ValueVector*> termsProperties;
    common::column_id_t dfColumnID;
    // AppearsIn table
    storage::RelTable* appearsInTable;
    common::ValueVector appearsInSrcVector;
    common::ValueVector appearsInDstVector;
    common::ValueVector relIDVector;
    common::ValueVector tfVector;
    std::vector<common::ValueVector*> appearsInProperties;
};

} // namespace fts_extension
} // namespace kuzu
