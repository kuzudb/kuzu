#pragma once

#include "storage/storage_structure/node_column.h"

namespace kuzu {
namespace storage {

class VarSizedNodeColumn : public NodeColumn {
public:
    VarSizedNodeColumn(common::LogicalType dataType,
        const catalog::MetaDiskArrayHeaderInfo& metaDAHeaderInfo, BMFileHandle* nodeGroupsDataFH,
        BMFileHandle* nodeGroupsMetaFH, BufferManager* bufferManager, WAL* wal);

protected:
    void scanInternal(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) final;
    void lookupInternal(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) final;
    inline void writeInternal(common::offset_t nodeOffset, common::ValueVector* vectorToWriteFrom,
        uint32_t posInVectorToWriteFrom) final {
        throw common::NotImplementedException("VarSizedNodeColumn write is not implemented yet");
    }

private:
    void readStringValueFromOvf(transaction::Transaction* transaction, common::ku_string_t& kuStr,
        common::ValueVector* resultVector, common::page_idx_t chunkStartPageIdx);
    void readListValueFromOvf(transaction::Transaction* transaction, common::ku_list_t kuList,
        common::ValueVector* resultVector, uint64_t posInVector,
        common::page_idx_t chunkStartPageIdx);

private:
    common::page_idx_t ovfPageIdxInChunk;
};

} // namespace storage
} // namespace kuzu
