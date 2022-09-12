#pragma once

#include "src/catalog/include/catalog.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/source_operator.h"
#include "src/storage/include/storage_manager.h"

using namespace graphflow::catalog;

namespace graphflow {
namespace processor {

class DropTable : public PhysicalOperator, public SourceOperator {

public:
    DropTable(Catalog* catalog, TableSchema* tableSchema, StorageManager& storageManager,
        uint32_t id, const string& paramsString)
        : PhysicalOperator{id, paramsString}, SourceOperator{nullptr}, catalog{catalog},
          tableSchema{tableSchema}, storageManager{storageManager} {}

    PhysicalOperatorType getOperatorType() override { return DROP_TABLE; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override { return nullptr; };

    inline double getExecutionTime(Profiler& profiler) const override {
        return profiler.sumAllTimeMetricsWithKey(getTimeMetricKey());
    }

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<DropTable>(catalog, tableSchema, storageManager, id, paramsString);
    }

    // TODO(Ziyi): Move those functions to wal_replayer_utils.cpp once we have implemented
    // transaction for drop table.
    static void removeListFilesIfExists(string fileName) {
        FileUtils::removeFileIfExists(fileName);
        FileUtils::removeFileIfExists(StorageUtils::getListMetadataFName(fileName));
        FileUtils::removeFileIfExists(StorageUtils::getOverflowPagesFName(fileName));
        FileUtils::removeFileIfExists(StorageUtils::getListHeadersFName(fileName));
    }

    static void removeColumnFilesIfExists(string fileName) {
        FileUtils::removeFileIfExists(fileName);
        FileUtils::removeFileIfExists(StorageUtils::getOverflowPagesFName(fileName));
    }

    static void removeDBFilesForNodeTable(NodeTableSchema* tableSchema, string directory) {
        for (auto& property : tableSchema->structuredProperties) {
            auto fName = StorageUtils::getNodePropertyColumnFName(
                directory, tableSchema->tableID, property.propertyID, DBFileType::ORIGINAL);
            removeColumnFilesIfExists(fName);
        }
        removeListFilesIfExists(StorageUtils::getNodeUnstrPropertyListsFName(
            directory, tableSchema->tableID, DBFileType::ORIGINAL));
        removeColumnFilesIfExists(
            StorageUtils::getNodeIndexFName(directory, tableSchema->tableID, DBFileType::ORIGINAL));
    }

    static void removeRelPropertyFiles(RelTableSchema* tableSchema, table_id_t nodeTableID,
        const string& directory, RelDirection relDirection, bool isColumnProperty) {
        for (auto i = 0u; i < tableSchema->getNumProperties(); ++i) {
            auto property = tableSchema->properties[i];
            if (isColumnProperty) {
                removeColumnFilesIfExists(
                    StorageUtils::getRelPropertyColumnFName(directory, tableSchema->tableID,
                        nodeTableID, relDirection, property.name, DBFileType::ORIGINAL));
            } else {
                removeListFilesIfExists(StorageUtils::getRelPropertyListsFName(directory,
                    tableSchema->tableID, nodeTableID, relDirection,
                    tableSchema->properties[i].propertyID, DBFileType::ORIGINAL));
            }
        }
    }

    static void removeDBFilesForRelTable(
        RelTableSchema* tableSchema, string directory, const Catalog* catalog) {
        for (auto relDirection : REL_DIRECTIONS) {
            auto nodeTableIDs = catalog->getReadOnlyVersion()->getNodeTableIDsForRelTableDirection(
                tableSchema->tableID, relDirection);
            for (auto nodeTableID : nodeTableIDs) {
                auto isColumnProperty =
                    catalog->getReadOnlyVersion()->isSingleMultiplicityInDirection(
                        tableSchema->tableID, relDirection);
                if (isColumnProperty) {
                    removeColumnFilesIfExists(StorageUtils::getAdjColumnFName(directory,
                        tableSchema->tableID, nodeTableID, relDirection, DBFileType::ORIGINAL));
                } else {
                    removeListFilesIfExists(StorageUtils::getAdjListsFName(directory,
                        tableSchema->tableID, nodeTableID, relDirection, DBFileType::ORIGINAL));
                }
                removeRelPropertyFiles(
                    tableSchema, nodeTableID, directory, relDirection, isColumnProperty);
            }
        }
    }

protected:
    Catalog* catalog;
    TableSchema* tableSchema;
    StorageManager& storageManager;
};

} // namespace processor
} // namespace graphflow
