#pragma once

#include <atomic>
#include <mutex>

#include "catalog/table_schema.h"
#include "common/ser_deser.h"
#include "storage/stats/property_statistics.h"
#include "transaction/transaction.h"

namespace kuzu {
namespace storage {

// DAH is the abbreviation for Disk Array Header.
class MetadataDAHInfo {
public:
    MetadataDAHInfo() : MetadataDAHInfo{common::INVALID_PAGE_IDX, common::INVALID_PAGE_IDX} {}
    MetadataDAHInfo(common::page_idx_t dataDAHPageIdx)
        : MetadataDAHInfo{dataDAHPageIdx, common::INVALID_PAGE_IDX} {}
    MetadataDAHInfo(common::page_idx_t dataDAHPageIdx, common::page_idx_t nullDAHPageIdx)
        : dataDAHPageIdx{dataDAHPageIdx}, nullDAHPageIdx{nullDAHPageIdx} {}

    inline std::unique_ptr<MetadataDAHInfo> copy() {
        auto result = std::make_unique<MetadataDAHInfo>(dataDAHPageIdx, nullDAHPageIdx);
        result->childrenInfos.resize(childrenInfos.size());
        for (size_t i = 0; i < childrenInfos.size(); ++i) {
            result->childrenInfos[i] = childrenInfos[i]->copy();
        }
        return result;
    }

    void serialize(common::FileInfo* fileInfo, uint64_t& offset) const;
    static std::unique_ptr<MetadataDAHInfo> deserialize(
        common::FileInfo* fileInfo, uint64_t& offset);

    common::page_idx_t dataDAHPageIdx = common::INVALID_PAGE_IDX;
    common::page_idx_t nullDAHPageIdx = common::INVALID_PAGE_IDX;
    std::vector<std::unique_ptr<MetadataDAHInfo>> childrenInfos;
};

class WAL;
class TableStatistics {
public:
    explicit TableStatistics(const catalog::TableSchema& schema)
        : tableType{schema.tableType}, numTuples{0}, tableID{schema.tableID} {
        for (auto property : schema.getProperties()) {
            propertyStatistics[property->getPropertyID()] = std::make_unique<PropertyStatistics>();
        }
    }

    TableStatistics(common::TableType tableType, uint64_t numTuples, common::table_id_t tableID,
        std::unordered_map<common::property_id_t, std::unique_ptr<PropertyStatistics>>&&
            propertyStatistics)
        : tableType{tableType}, numTuples{numTuples}, tableID{tableID},
          propertyStatistics{std::move(propertyStatistics)} {
        assert(numTuples != UINT64_MAX);
    }

    explicit TableStatistics(const TableStatistics& other)
        : tableType{other.tableType}, numTuples{other.numTuples}, tableID{other.tableID} {
        for (auto& propertyStats : other.propertyStatistics) {
            propertyStatistics[propertyStats.first] =
                std::make_unique<PropertyStatistics>(*propertyStats.second.get());
        }
    }

    virtual ~TableStatistics() = default;

    inline bool isEmpty() const { return numTuples == 0; }
    inline uint64_t getNumTuples() const { return numTuples; }
    virtual inline void setNumTuples(uint64_t numTuples_) {
        assert(numTuples_ != UINT64_MAX);
        numTuples = numTuples_;
    }

    inline PropertyStatistics& getPropertyStatistics(common::property_id_t propertyID) {
        assert(propertyStatistics.contains(propertyID));
        return *(propertyStatistics.at(propertyID));
    }
    inline const std::unordered_map<common::property_id_t, std::unique_ptr<PropertyStatistics>>&
    getPropertyStatistics() {
        return propertyStatistics;
    }
    inline void setPropertyStatistics(
        common::property_id_t propertyID, PropertyStatistics newStats) {
        propertyStatistics[propertyID] = std::make_unique<PropertyStatistics>(newStats);
    }

    void serialize(common::FileInfo* fileInfo, uint64_t& offset);
    static std::unique_ptr<TableStatistics> deserialize(
        common::FileInfo* fileInfo, uint64_t& offset);
    virtual void serializeInternal(common::FileInfo* fileInfo, uint64_t& offset) = 0;

    virtual std::unique_ptr<TableStatistics> copy() = 0;

private:
    common::TableType tableType;
    uint64_t numTuples;
    common::table_id_t tableID;
    std::unordered_map<common::property_id_t, std::unique_ptr<PropertyStatistics>>
        propertyStatistics;
};

struct TablesStatisticsContent {
    TablesStatisticsContent() = default;
    std::unordered_map<common::table_id_t, std::unique_ptr<TableStatistics>> tableStatisticPerTable;
};

class TablesStatistics {
public:
    TablesStatistics(BMFileHandle* metadataFH);

    virtual ~TablesStatistics() = default;

    virtual void setNumTuplesForTable(common::table_id_t tableID, uint64_t numTuples) = 0;

    inline void writeTablesStatisticsFileForWALRecord(const std::string& directory) {
        saveToFile(directory, common::DBFileType::WAL_VERSION, transaction::TransactionType::WRITE);
    }

    inline bool hasUpdates() { return tablesStatisticsContentForWriteTrx != nullptr; }

    inline void checkpointInMemoryIfNecessary() {
        std::unique_lock lck{mtx};
        tablesStatisticsContentForReadOnlyTrx = std::move(tablesStatisticsContentForWriteTrx);
    }

    inline TablesStatisticsContent* getReadOnlyVersion() const {
        return tablesStatisticsContentForReadOnlyTrx.get();
    }

    inline void addTableStatistic(catalog::TableSchema* tableSchema) {
        initTableStatisticsForWriteTrx();
        tablesStatisticsContentForWriteTrx->tableStatisticPerTable[tableSchema->tableID] =
            constructTableStatistic(tableSchema);
    }
    inline void removeTableStatistic(common::table_id_t tableID) {
        tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable.erase(tableID);
    }

    inline uint64_t getNumTuplesForTable(common::table_id_t tableID) {
        return tablesStatisticsContentForReadOnlyTrx->tableStatisticPerTable[tableID]
            ->getNumTuples();
    }

    PropertyStatistics& getPropertyStatisticsForTable(const transaction::Transaction& transaction,
        common::table_id_t tableID, common::property_id_t propertyID);

    void setPropertyStatisticsForTable(
        common::table_id_t tableID, common::property_id_t propertyID, PropertyStatistics stats);

    static std::unique_ptr<MetadataDAHInfo> createMetadataDAHInfo(
        const common::LogicalType& dataType, BMFileHandle& metadataFH, BufferManager* bm, WAL* wal);

protected:
    virtual std::unique_ptr<TableStatistics> constructTableStatistic(
        catalog::TableSchema* tableSchema) = 0;

    virtual std::unique_ptr<TableStatistics> constructTableStatistic(
        TableStatistics* tableStatistics) = 0;

    virtual std::string getTableStatisticsFilePath(
        const std::string& directory, common::DBFileType dbFileType) = 0;

    void readFromFile(const std::string& directory);
    void readFromFile(const std::string& directory, common::DBFileType dbFileType);

    void saveToFile(const std::string& directory, common::DBFileType dbFileType,
        transaction::TransactionType transactionType);

    void initTableStatisticsForWriteTrx();
    void initTableStatisticsForWriteTrxNoLock();

protected:
    BMFileHandle* metadataFH;
    std::unique_ptr<TablesStatisticsContent> tablesStatisticsContentForReadOnlyTrx;
    std::unique_ptr<TablesStatisticsContent> tablesStatisticsContentForWriteTrx;
    std::mutex mtx;
};

} // namespace storage
} // namespace kuzu
