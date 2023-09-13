#pragma once

#include "common/copier_config/copier_config.h"
#include "common/task_system/task_scheduler.h"
#include "processor/operator/persistent/csv_file_writer.h"
#include "processor/operator/persistent/file_writer.h"
#include "processor/operator/persistent/parquet_file_writer.h"
#include "processor/operator/physical_operator.h"
#include "processor/operator/sink.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace processor {

class CopyToSharedState {
public:
    CopyToSharedState(common::CopyDescription::FileType fileType, std::string& filePath,
        std::vector<std::string>& columnNames, std::vector<common::LogicalType>& columnTypes) {
        if (fileType == common::CopyDescription::FileType::CSV) {
            fileWriter = std::make_unique<kuzu::processor::CSVFileWriter>(
                filePath, columnNames, columnTypes);
        } else if (fileType == common::CopyDescription::FileType::PARQUET) {
            fileWriter = std::make_unique<kuzu::processor::ParquetFileWriter>(
                filePath, columnNames, columnTypes);
        } else {
            throw common::NotImplementedException("CopyToSharedState::CopyToSharedState");
        }
    }
    inline std::unique_ptr<FileWriter>& getWriter() { return fileWriter; }

private:
    std::unique_ptr<FileWriter> fileWriter;
};

class CopyTo : public Sink {
public:
    CopyTo(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::shared_ptr<CopyToSharedState> sharedState, std::vector<DataPos> vectorsToCopyPos,
        const common::CopyDescription& copyDescription, uint32_t id,
        const std::string& paramsString, std::unique_ptr<PhysicalOperator> child)
        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::COPY_TO, std::move(child), id,
              paramsString},
          sharedState{std::move(sharedState)}, vectorsToCopyPos{std::move(vectorsToCopyPos)},
          copyDescription{copyDescription} {}

    inline bool canParallel() const final { return false; }

    void executeInternal(ExecutionContext* context) final;

    void finalize(ExecutionContext* context) final;

    common::CopyDescription& getCopyDescription() { return copyDescription; }

    std::unique_ptr<PhysicalOperator> clone() final {
        return make_unique<CopyTo>(resultSetDescriptor->copy(), sharedState, vectorsToCopyPos,
            copyDescription, id, paramsString, children[0]->clone());
    }

protected:
    std::string getOutputMsg();
    std::vector<DataPos> vectorsToCopyPos;

private:
    void initGlobalStateInternal(ExecutionContext* context) override;
    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;
    common::CopyDescription copyDescription;
    std::vector<common::ValueVector*> outputVectors;
    std::shared_ptr<CopyToSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu
