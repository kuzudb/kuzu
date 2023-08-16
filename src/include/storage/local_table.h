#pragma once

#include <bitset>

#include "common/vector/value_vector.h"

namespace kuzu {
namespace storage {
class NodeColumn;
class NodeTable;

class LocalVector {
public:
    LocalVector(const common::LogicalType& logicalType, storage::MemoryManager* mm) {
        vector = std::make_unique<common::ValueVector>(logicalType, mm);
        vector->setState(std::make_shared<common::DataChunkState>());
        vector->state->selVector->resetSelectorToValuePosBuffer();
    }

    virtual ~LocalVector() = default;

    void scan(common::ValueVector* resultVector) const;
    void lookup(common::sel_t offsetInLocalVector, common::ValueVector* resultVector,
        common::sel_t offsetInResultVector);
    virtual void update(common::sel_t offsetInLocalVector, common::ValueVector* updateVector,
        common::sel_t offsetInUpdateVector);

    std::unique_ptr<common::ValueVector> vector;
    // This mask is mainly to speed the lookup operation up. Otherwise, we have to do binary search
    // to check if the value at an offset has been updated or not.
    std::bitset<common::DEFAULT_VECTOR_CAPACITY> validityMask;
};

class StringLocalVector : public LocalVector {
public:
    explicit StringLocalVector(storage::MemoryManager* mm)
        : LocalVector{common::LogicalType(common::LogicalTypeID::STRING), mm}, ovfStringLength{
                                                                                   0} {};

    void update(common::sel_t offsetInLocalVector, common::ValueVector* updateVector,
        common::sel_t offsetInUpdateVector) final;

    uint64_t ovfStringLength;
};

struct LocalVectorFactory {
    static std::unique_ptr<LocalVector> createLocalVectorData(
        const common::LogicalType& logicalType, storage::MemoryManager* mm);
};

class LocalColumnChunk {
public:
    explicit LocalColumnChunk(storage::MemoryManager* mm) : mm{mm} {};

    void scan(common::vector_idx_t vectorIdx, common::ValueVector* resultVector);
    void lookup(common::vector_idx_t vectorIdx, common::sel_t offsetInLocalVector,
        common::ValueVector* resultVector, common::sel_t offsetInResultVector);
    void update(common::vector_idx_t vectorIdx, common::sel_t offsetInVector,
        common::ValueVector* vectorToWriteFrom, common::sel_t pos);

    std::map<common::vector_idx_t, std::unique_ptr<LocalVector>> vectors;
    storage::MemoryManager* mm;
};

class LocalColumn {
public:
    explicit LocalColumn(storage::NodeColumn* column) : column{column} {};
    virtual ~LocalColumn() = default;

    void scan(common::ValueVector* nodeIDVector, common::ValueVector* resultVector);
    void lookup(common::ValueVector* nodeIDVector, common::ValueVector* resultVector);
    void update(common::ValueVector* nodeIDVector, common::ValueVector* propertyVector,
        storage::MemoryManager* mm);
    void update(common::offset_t nodeOffset, common::ValueVector* propertyVector,
        common::sel_t posInPropertyVector, storage::MemoryManager* mm);

    virtual void prepareCommit();

protected:
    virtual void prepareCommitForChunk(common::node_group_idx_t nodeGroupIdx);

protected:
    std::map<common::node_group_idx_t, std::unique_ptr<LocalColumnChunk>> chunks;
    storage::NodeColumn* column;
};

class StringLocalColumn : public LocalColumn {
public:
    explicit StringLocalColumn(storage::NodeColumn* column) : LocalColumn{column} {};

private:
    void prepareCommitForChunk(common::node_group_idx_t nodeGroupIdx) final;
    void commitLocalChunkOutOfPlace(
        common::node_group_idx_t nodeGroupIdx, LocalColumnChunk* localChunk);
};

class VarListLocalColumn : public LocalColumn {
public:
    explicit VarListLocalColumn(storage::NodeColumn* column) : LocalColumn{column} {};

private:
    void prepareCommitForChunk(common::node_group_idx_t nodeGroupIdx) final;
};

struct LocalColumnFactory {
    static std::unique_ptr<LocalColumn> createLocalColumn(storage::NodeColumn* column);
};

class LocalTable {
public:
    explicit LocalTable(storage::NodeTable* table) : table{table} {};

    void scan(common::ValueVector* nodeIDVector, const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);
    void lookup(common::ValueVector* nodeIDVector,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);
    void update(common::property_id_t propertyID, common::ValueVector* nodeIDVector,
        common::ValueVector* propertyVector, storage::MemoryManager* mm);
    void update(common::property_id_t propertyID, common::offset_t nodeOffset,
        common::ValueVector* propertyVector, common::sel_t posInPropertyVector,
        storage::MemoryManager* mm);

    void prepareCommit();

private:
    std::map<common::property_id_t, std::unique_ptr<LocalColumn>> columns;
    storage::NodeTable* table;
};

} // namespace storage
} // namespace kuzu
