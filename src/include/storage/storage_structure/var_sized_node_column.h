#pragma once

#include "storage/storage_structure/node_column.h"

namespace kuzu {
namespace storage {

struct VarSizedNodeColumnFunc {
    static void writeStringValuesToPage(
        uint8_t* frame, uint16_t posInFrame, common::ValueVector* vector, uint32_t posInVector);
};

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
