#include "src/processor/include/physical_plan/operator/drop_table.h"

namespace graphflow {
namespace processor {

bool DropTable::getNextTuples() {
    // TODO(Ziyi): Remove this if/else code once we have implemented transaction for drop table.
    if (tableSchema->isNodeTable) {
        removeDBFilesForNodeTable((NodeTableSchema*)tableSchema, storageManager.getDirectory());
        storageManager.getNodesStore().removeNodeTable(tableSchema->tableID);
    } else {
        removeDBFilesForRelTable(
            (RelTableSchema*)tableSchema, storageManager.getDirectory(), catalog);
        storageManager.getRelsStore().removeRelTable(tableSchema->tableID);
    }
    catalog->removeTableSchema(tableSchema);
    return false;
}

} // namespace processor
} // namespace graphflow
