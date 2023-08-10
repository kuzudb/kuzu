#pragma once

#include "common/copier_config/copier_config.h"
#include "common/task_system/task_scheduler.h"
#include "processor/operator/copy_to/csv_file_writer.h"
#include "processor/operator/physical_operator.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace processor {

class WriteCSVFileSharedState {
public:
    WriteCSVFileSharedState() {
        csvFileWriter = std::make_unique<kuzu::processor::CSVFileWriter>();
    };
    std::unique_ptr<kuzu::processor::CSVFileWriter> csvFileWriter;
};

class CopyTo : public PhysicalOperator {
public:
    CopyTo(std::shared_ptr<WriteCSVFileSharedState> sharedState,
        std::vector<DataPos> vectorsToCopyPos, const common::CopyDescription& copyDescription,
        uint32_t id, const std::string& paramsString, std::unique_ptr<PhysicalOperator> child)
        : PhysicalOperator{PhysicalOperatorType::COPY_TO, std::move(child), id, paramsString},
          sharedState{std::move(sharedState)}, vectorsToCopyPos{std::move(vectorsToCopyPos)},
          copyDescription{copyDescription} {}

    bool getNextTuplesInternal(ExecutionContext* context) override;

    common::CopyDescription& getCopyDescription() { return copyDescription; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CopyTo>(
            sharedState, vectorsToCopyPos, copyDescription, id, paramsString, children[0]->clone());
    }

protected:
    std::string getOutputMsg();
    std::vector<DataPos> vectorsToCopyPos;

private:
    void initGlobalStateInternal(ExecutionContext* context) override;
    common::CopyDescription copyDescription;
    std::vector<common::ValueVector*> outputVectors;
    std::shared_ptr<WriteCSVFileSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu
