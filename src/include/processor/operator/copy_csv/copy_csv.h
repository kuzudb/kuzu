#pragma once

#include "common/csv_reader/csv_reader.h"
#include "common/task_system/task_scheduler.h"
#include "processor/operator/physical_operator.h"
#include "storage/store/nodes_statistics_and_deleted_ids.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

class CopyCSV : public PhysicalOperator {
public:
    CopyCSV(PhysicalOperatorType operatorType, Catalog* catalog, CSVDescription csvDescription,
        TableSchema tableSchema, WAL* wal, uint32_t id, const string& paramsString)
        : PhysicalOperator{operatorType, id, paramsString}, catalog{catalog},
          csvDescription{std::move(csvDescription)}, tableSchema{std::move(tableSchema)}, wal{wal} {
    }
    virtual ~CopyCSV() override = default;

    inline bool isSource() const override { return true; }

    virtual string execute(TaskScheduler* taskScheduler, ExecutionContext* executionContext) = 0;

    bool getNextTuplesInternal() override {
        throw InternalException("getNextTupleInternal() should not be called on CopyCSV operator.");
    }

protected:
    void errorIfTableIsNonEmpty(TablesStatistics* tablesStatistics) {
        auto numTuples = tablesStatistics->getReadOnlyVersion()
                             ->tableStatisticPerTable.at(tableSchema.tableID)
                             ->getNumTuples();
        if (numTuples > 0) {
            throw CopyCSVException(
                "COPY CSV commands can be executed only on completely empty tables. Table: " +
                tableSchema.tableName + " has " + to_string(numTuples) + " many tuples.");
        }
    }

protected:
    Catalog* catalog;
    CSVDescription csvDescription;
    TableSchema tableSchema;
    WAL* wal;
};

} // namespace processor
} // namespace kuzu
