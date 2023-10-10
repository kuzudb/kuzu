#pragma once

#include "common/copier_config/copier_config.h"
#include "common/task_system/task_scheduler.h"
#include "processor/operator/persistent/csv_file_writer.h"
#include "processor/operator/persistent/file_writer.h"
#include "processor/operator/physical_operator.h"
#include "processor/operator/sink.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace processor {

class CopyToSharedState {
public:
    CopyToSharedState(common::FileType fileType, std::string& filePath,
        std::vector<std::string>& columnNames,
        std::vector<std::unique_ptr<common::LogicalType>> columnTypes) {
        assert(fileType == common::FileType::CSV);
        fileWriter = std::make_unique<processor::CSVFileWriter>(
            filePath, columnNames, std::move(columnTypes));
    }
    inline std::unique_ptr<FileWriter>& getWriter() { return fileWriter; }

private:
    std::unique_ptr<FileWriter> fileWriter;
};

class CopyTo : public Sink {
public:
    CopyTo(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::shared_ptr<CopyToSharedState> sharedState, std::vector<DataPos> vectorsToCopyPos,
        uint32_t id, const std::string& paramsString, std::unique_ptr<PhysicalOperator> child)
        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::COPY_TO, std::move(child), id,
              paramsString},
          sharedState{std::move(sharedState)}, vectorsToCopyPos{std::move(vectorsToCopyPos)} {}

    inline bool canParallel() const final { return false; }

    void executeInternal(ExecutionContext* context) final;

    void finalize(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final {
        return make_unique<CopyTo>(resultSetDescriptor->copy(), sharedState, vectorsToCopyPos, id,
            paramsString, children[0]->clone());
    }

protected:
    std::vector<DataPos> vectorsToCopyPos;

private:
    void initGlobalStateInternal(ExecutionContext* context) override;
    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;
    std::vector<common::ValueVector*> outputVectors;
    std::shared_ptr<CopyToSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu
