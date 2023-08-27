#pragma once

#include "common/copier_config/copier_config.h"
#include "common/task_system/task_scheduler.h"
#include "processor/operator/physical_operator.h"
#include "storage/store/nodes_statistics_and_deleted_ids.h"

namespace kuzu {
namespace processor {

class Copy : public PhysicalOperator {
public:
    Copy(PhysicalOperatorType operatorType, catalog::Catalog* catalog,
        const common::CopyDescription& copyDescription, common::table_id_t tableID,
        storage::WAL* wal, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{operatorType, id, paramsString}, catalog{catalog},
          copyDescription{copyDescription}, tableID{tableID}, wal{wal} {}

    inline bool isSource() const override { return true; }

    std::string execute(common::TaskScheduler* taskScheduler, ExecutionContext* executionContext);

    bool getNextTuplesInternal(ExecutionContext* context) override {
        throw common::InternalException(
            "getNextTupleInternal() should not be called on CopyCSV operator.");
    }

protected:
    std::string getOutputMsg(uint64_t numTuplesCopied);

    virtual uint64_t executeInternal(
        common::TaskScheduler* taskScheduler, ExecutionContext* executionContext) = 0;

    virtual bool isCopyAllowed() = 0;

protected:
    catalog::Catalog* catalog;
    common::CopyDescription copyDescription;
    common::table_id_t tableID;
    storage::WAL* wal;
};

} // namespace processor
} // namespace kuzu
