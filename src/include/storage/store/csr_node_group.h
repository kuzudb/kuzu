#pragma once

#include <map>

#include "storage/storage_utils.h"
#include "storage/store/chunked_node_group_collection.h"
#include "storage/store/csr_chunked_node_group.h"

namespace kuzu {
namespace storage {

using row_idx_vec_t = std::vector<common::row_idx_t>;

struct csr_list_t {
    common::row_idx_t startRow = common::INVALID_ROW_IDX;
    common::length_t length = 0;
};

enum class CSRNodeGroupScanSource : uint8_t {
    COMMITTED_AND_CHECKPOINTED = 0,
    COMMITTED_NOT_CHECKPOINTED = 1
};

class CSRNodeGroup;
struct CSRNodeGroupScanState {
    // States at the node group level. Cached during scan over all csr lists within the same node
    // group. State for reading from checkpointed node group.
    std::unique_ptr<ChunkedCSRHeader> checkpointedHeader;
    // State of each chunk in the checkpointed chunked group.
    std::vector<ChunkState> chunkStates;

    // States at the csr list level. Cached during scan over a single csr list.
    CSRNodeGroupScanSource source = CSRNodeGroupScanSource::COMMITTED_AND_CHECKPOINTED;
    common::row_idx_t nextRowToScan = 0;
    csr_list_t checkpointedCSRList;
    row_idx_vec_t committedNotCheckpointedRows;

    void resetState() {
        checkpointedHeader.reset();
        source = CSRNodeGroupScanSource::COMMITTED_AND_CHECKPOINTED;
        nextRowToScan = 0;
        checkpointedCSRList = csr_list_t();
        committedNotCheckpointedRows.clear();
        for (auto& chunkState : chunkStates) {
            chunkState.resetState();
        }
    }
};

struct CSRIndex {
    std::map<common::offset_t, row_idx_vec_t> offsetToRowIdxs;
};

class CSRNodeGroup {
public:
    CSRNodeGroup(const common::node_group_idx_t nodeGroupIdx, const bool enableCompression,
        const std::vector<common::LogicalType>& dataTypes)
        : nodeGroupIdx{nodeGroupIdx}, enableCompression{enableCompression}, dataTypes{dataTypes},
          startNodeOffset{StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx)} {}
    CSRNodeGroup(const common::node_group_idx_t nodeGroupIdx, const bool enableCompression,
        std::unique_ptr<ChunkedCSRNodeGroup> chunkedNodeGroup)
        : nodeGroupIdx{nodeGroupIdx}, enableCompression{enableCompression},
          startNodeOffset{StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx)},
          checkpointedGroup{std::move(chunkedNodeGroup)} {
        for (auto i = 0u; i < checkpointedGroup->getNumColumns(); i++) {
            dataTypes.push_back(checkpointedGroup->getColumnChunk(i).getDataType());
        }
    }

    common::row_idx_t getNumRows() const { return numRows.load(); }

    void initializeScanState(transaction::Transaction* transaction, TableScanState& state);
    bool scan(transaction::Transaction* transaction, TableScanState& state);

    void serialize(common::Serializer& serializer) const;
    static std::unique_ptr<CSRNodeGroup> deserialize(common::Deserializer& deSer);

    common::offset_t getStartNodeOffset() const { return startNodeOffset; }

private:
    common::node_group_idx_t nodeGroupIdx;
    bool enableCompression;
    std::atomic<common::row_idx_t> numRows;
    std::vector<common::LogicalType> dataTypes;
    // Offset of the first node in the group.
    common::offset_t startNodeOffset;

    std::unique_ptr<ChunkedCSRNodeGroup> checkpointedGroup;
    CSRIndex csrIndex;
    GroupCollection<ChunkedNodeGroup> chunkedGroups;
};

} // namespace storage
} // namespace kuzu
