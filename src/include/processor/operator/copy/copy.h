#pragma once

#include "common/csv_reader/csv_reader.h"
#include "common/task_system/task_scheduler.h"
#include "processor/operator/physical_operator.h"
#include "storage/store/nodes_statistics_and_deleted_ids.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

class Copy : public PhysicalOperator {
public:
    Copy(PhysicalOperatorType operatorType, Catalog* catalog, CopyDescription copyDescription,
        table_id_t tableID, WAL* wal, uint32_t id, const string& paramsString)
        : PhysicalOperator{operatorType, id, paramsString}, catalog{catalog},
          copyDescription{std::move(copyDescription)}, tableID{tableID}, wal{wal} {}

    inline bool isSource() const override { return true; }

    string execute(TaskScheduler* taskScheduler, ExecutionContext* executionContext);

    bool getNextTuplesInternal() override {
        throw InternalException("getNextTupleInternal() should not be called on CopyCSV operator.");
    }

protected:
    void errorIfTableIsNonEmpty();

    std::string getOutputMsg(uint64_t numTuplesCopied);

    virtual uint64_t executeInternal(
        TaskScheduler* taskScheduler, ExecutionContext* executionContext) = 0;

    virtual uint64_t getNumTuplesInTable() = 0;

protected:
    Catalog* catalog;
    CopyDescription copyDescription;
    table_id_t tableID;
    WAL* wal;
};

} // namespace processor
} // namespace kuzu
