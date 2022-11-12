#pragma once

#include "src/common/include/csv_reader/csv_reader.h"
#include "src/common/include/task_system/task_scheduler.h"
#include "src/processor/operator/include/physical_operator.h"
#include "src/storage/store/include/nodes_statistics_and_deleted_ids.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

class CopyCSV : public PhysicalOperator {

public:
    CopyCSV(Catalog* catalog, CSVDescription csvDescription, TableSchema tableSchema, WAL* wal,
        uint32_t id, const string& paramsString)
        : PhysicalOperator{id, paramsString}, catalog{catalog},
          csvDescription{move(csvDescription)}, tableSchema{move(tableSchema)}, wal{wal} {}

    virtual string execute(TaskScheduler* taskScheduler, ExecutionContext* executionContext) = 0;

    bool getNextTuples() override { assert(false); }

    virtual ~CopyCSV() = default;

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
