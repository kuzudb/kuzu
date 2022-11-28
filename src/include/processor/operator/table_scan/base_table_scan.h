#pragma once

#include "processor/operator/result_collector.h"
#include "processor/operator/source_operator.h"

namespace kuzu {
namespace processor {

class BaseTableScan : public PhysicalOperator, public SourceOperator {
protected:
    // For factorized table scan of all columns
    BaseTableScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        vector<DataPos> outVecPositions, vector<DataType> outVecDataTypes,
        vector<uint32_t> colIndicesToScan, unique_ptr<PhysicalOperator> child, uint32_t id,
        const string& paramsString)
        : PhysicalOperator{std::move(child), id, paramsString}, SourceOperator{std::move(
                                                                    resultSetDescriptor)},
          maxMorselSize{0}, outVecPositions{std::move(outVecPositions)},
          outVecDataTypes{std::move(outVecDataTypes)}, colIndicesToScan{
                                                           std::move(colIndicesToScan)} {}

    // For factorized table scan of some columns
    BaseTableScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        vector<DataPos> outVecPositions, vector<DataType> outVecDataTypes,
        vector<uint32_t> colIndicesToScan, uint32_t id, const string& paramsString)
        : PhysicalOperator{id, paramsString}, SourceOperator{std::move(resultSetDescriptor)},
          maxMorselSize{0}, outVecPositions{std::move(outVecPositions)},
          outVecDataTypes{std::move(outVecDataTypes)}, colIndicesToScan{
                                                           std::move(colIndicesToScan)} {}

    // For union all scan
    BaseTableScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        vector<DataPos> outVecPositions, vector<DataType> outVecDataTypes,
        vector<uint32_t> colIndicesToScan, vector<unique_ptr<PhysicalOperator>> children,
        uint32_t id, const string& paramsString)
        : PhysicalOperator{std::move(children), id, paramsString}, SourceOperator{std::move(
                                                                       resultSetDescriptor)},
          maxMorselSize{0}, outVecPositions{std::move(outVecPositions)},
          outVecDataTypes{std::move(outVecDataTypes)}, colIndicesToScan{
                                                           std::move(colIndicesToScan)} {}

    virtual void setMaxMorselSize() = 0;
    virtual unique_ptr<FTableScanMorsel> getMorsel() = 0;

    void initFurther(ExecutionContext* context);

    bool getNextTuplesInternal() override;

    inline double getExecutionTime(Profiler& profiler) const override {
        return profiler.sumAllTimeMetricsWithKey(getTimeMetricKey());
    }

protected:
    uint64_t maxMorselSize;
    vector<DataPos> outVecPositions;
    vector<DataType> outVecDataTypes;
    vector<uint32_t> colIndicesToScan;

    vector<shared_ptr<ValueVector>> vectorsToScan;
};

} // namespace processor
} // namespace kuzu
