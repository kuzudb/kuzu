#pragma once

#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/mask.h"
#include "storage/predicate/column_predicate.h"
#include "storage/store/column.h"
#include "storage/store/node_group.h"

namespace kuzu {
namespace evaluator {
class ExpressionEvaluator;
} // namespace evaluator
namespace storage {
class MemoryManager;

enum class TableScanSource : uint8_t { COMMITTED = 0, UNCOMMITTED = 1, NONE = UINT8_MAX };

struct TableScanState {
    common::table_id_t tableID;
    std::unique_ptr<common::ValueVector> rowIdxVector;
    // Node/Rel ID vector. We assume all output vectors are within the same DataChunk as this one.
    common::ValueVector* nodeIDVector;
    std::vector<common::ValueVector*> outputVectors;
    common::DataChunkState* outState;
    std::vector<common::column_id_t> columnIDs;
    common::NodeSemiMask* semiMask;

    // Only used when scan from persistent data.
    std::vector<const Column*> columns;

    TableScanSource source = TableScanSource::NONE;
    common::node_group_idx_t nodeGroupIdx = common::INVALID_NODE_GROUP_IDX;
    NodeGroup* nodeGroup = nullptr;
    std::unique_ptr<NodeGroupScanState> nodeGroupScanState;

    std::vector<ColumnPredicateSet> columnPredicateSets;

    TableScanState(common::table_id_t tableID, std::vector<common::column_id_t> columnIDs)
        : TableScanState{tableID, std::move(columnIDs), {}} {}
    TableScanState(common::table_id_t tableID, std::vector<common::column_id_t> columnIDs,
        std::vector<const Column*> columns)
        : TableScanState{tableID, std::move(columnIDs), std::move(columns), {}} {}
    TableScanState(common::table_id_t tableID, std::vector<common::column_id_t> columnIDs,
        std::vector<const Column*> columns, std::vector<ColumnPredicateSet> columnPredicateSets)
        : tableID{tableID}, nodeIDVector(nullptr), outState{nullptr},
          columnIDs{std::move(columnIDs)}, semiMask{nullptr}, columns{std::move(columns)},
          columnPredicateSets{std::move(columnPredicateSets)} {
        rowIdxVector = std::make_unique<common::ValueVector>(common::LogicalType::INT64());
    }

    virtual ~TableScanState() = default;
    DELETE_COPY_DEFAULT_MOVE(TableScanState);

    virtual void initState(transaction::Transaction* transaction, NodeGroup* nodeGroup) {
        KU_ASSERT(nodeGroup);
        this->nodeGroup = nodeGroup;
        this->nodeGroup->initializeScanState(transaction, *this);
    }

    virtual bool scanNext(transaction::Transaction*) { KU_UNREACHABLE; }

    void resetOutVectors();

    virtual void resetState() {
        source = TableScanSource::NONE;
        nodeGroupIdx = common::INVALID_NODE_GROUP_IDX;
        nodeGroup = nullptr;
        nodeGroupScanState->resetState();
    }

    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<TARGET&>(*this);
    }
    template<class TARGETT>
    const TARGETT& cast() const {
        return common::ku_dynamic_cast<const TARGETT&>(*this);
    }
};

struct TableInsertState {
    const std::vector<common::ValueVector*>& propertyVectors;

    explicit TableInsertState(const std::vector<common::ValueVector*>& propertyVectors)
        : propertyVectors{propertyVectors} {}
    virtual ~TableInsertState() = default;

    template<typename T>
    const T& constCast() const {
        return common::ku_dynamic_cast<const T&>(*this);
    }
    template<typename T>
    T& cast() {
        return common::ku_dynamic_cast<T&>(*this);
    }
};

struct TableUpdateState {
    common::column_id_t columnID;
    common::ValueVector& propertyVector;

    TableUpdateState(common::column_id_t columnID, common::ValueVector& propertyVector)
        : columnID{columnID}, propertyVector{propertyVector} {}
    virtual ~TableUpdateState() = default;

    template<typename T>
    const T& constCast() const {
        return common::ku_dynamic_cast<const T&>(*this);
    }
    template<typename T>
    T& cast() {
        return common::ku_dynamic_cast<T&>(*this);
    }
};

struct TableDeleteState {
    virtual ~TableDeleteState() = default;

    template<typename T>
    const T& constCast() const {
        return common::ku_dynamic_cast<const T&>(*this);
    }
    template<typename T>
    T& cast() {
        return common::ku_dynamic_cast<T&>(*this);
    }
};

struct TableAddColumnState final {
    const binder::PropertyDefinition& propertyDefinition;
    evaluator::ExpressionEvaluator& defaultEvaluator;

    TableAddColumnState(const binder::PropertyDefinition& propertyDefinition,
        evaluator::ExpressionEvaluator& defaultEvaluator)
        : propertyDefinition{propertyDefinition}, defaultEvaluator{defaultEvaluator} {}
    ~TableAddColumnState() = default;
};

class LocalTable;
class StorageManager;
class Table {
public:
    Table(const catalog::TableCatalogEntry* tableEntry, const StorageManager* storageManager,
        MemoryManager* memoryManager);
    virtual ~Table() = default;

    static std::unique_ptr<Table> loadTable(common::Deserializer& deSer,
        const catalog::Catalog& catalog, StorageManager* storageManager,
        MemoryManager* memoryManager, common::VirtualFileSystem* vfs, main::ClientContext* context);

    common::TableType getTableType() const { return tableType; }
    common::table_id_t getTableID() const { return tableID; }
    std::string getTableName() const { return tableName; }
    FileHandle* getDataFH() const { return dataFH; }

    virtual void initScanState(transaction::Transaction* transaction,
        TableScanState& readState) const = 0;
    bool scan(transaction::Transaction* transaction, TableScanState& scanState);

    virtual void insert(transaction::Transaction* transaction, TableInsertState& insertState) = 0;
    virtual void update(transaction::Transaction* transaction, TableUpdateState& updateState) = 0;
    virtual bool delete_(transaction::Transaction* transaction, TableDeleteState& deleteState) = 0;

    virtual void addColumn(transaction::Transaction* transaction,
        TableAddColumnState& addColumnState) = 0;
    void dropColumn() { setHasChanges(); }

    virtual void commit(transaction::Transaction* transaction, LocalTable* localTable) = 0;
    virtual void checkpoint(common::Serializer& ser, catalog::TableCatalogEntry* tableEntry) = 0;

    virtual common::row_idx_t getNumRows() = 0;

    void setHasChanges() { hasChanges = true; }

    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<TARGET&>(*this);
    }
    template<class TARGET>
    const TARGET& cast() const {
        return common::ku_dynamic_cast<const TARGET&>(*this);
    }
    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<TARGET*>(this);
    }

    MemoryManager& getMemoryManager() const { return *memoryManager; }

protected:
    virtual bool scanInternal(transaction::Transaction* transaction, TableScanState& scanState) = 0;

    virtual void serialize(common::Serializer& serializer) const;

    std::unique_ptr<common::DataChunk> constructDataChunk(
        const std::vector<common::LogicalType>& types);

protected:
    common::TableType tableType;
    common::table_id_t tableID;
    std::string tableName;
    bool enableCompression;
    FileHandle* dataFH;
    MemoryManager* memoryManager;
    ShadowFile* shadowFile;
    bool hasChanges;
};

} // namespace storage
} // namespace kuzu
