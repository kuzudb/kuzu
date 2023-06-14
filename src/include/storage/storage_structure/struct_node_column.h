#include "storage/storage_structure/node_column.h"

namespace kuzu {
namespace storage {

class StructNodeColumn : public NodeColumn {
public:
    StructNodeColumn(common::LogicalType dataType,
        const catalog::MetaDiskArrayHeaderInfo& metaDAHeaderInfo, BMFileHandle* nodeGroupsDataFH,
        BMFileHandle* nodeGroupsMetaFH, BufferManager* bufferManager, WAL* wal);

protected:
    void scanInternal(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) final;
    void lookupInternal(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) final;
};

} // namespace storage
} // namespace kuzu
