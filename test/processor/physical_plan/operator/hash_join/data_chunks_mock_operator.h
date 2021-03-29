#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/tuple/data_chunks.h"

using namespace graphflow::processor;

namespace graphflow {
namespace processor {

class DataChunksMockOperator : public PhysicalOperator {
public:
    DataChunksMockOperator() { dataChunks = make_shared<DataChunks>(); }
    DataChunksMockOperator(shared_ptr<DataChunks> mockDataChunks) { dataChunks = mockDataChunks; }

    bool hasNextMorsel() override { return true; };
    void getNextTuples() override {}
    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<DataChunksMockOperator>(dataChunks);
    }
};

class ScanMockOp : public DataChunksMockOperator {
public:
    ScanMockOp() : numTuples(0) { generateDataChunks(); }

    void getNextTuples() override;

private:
    void generateDataChunks();

    uint64_t numTuples;
};

class ProbeScanMockOp : public DataChunksMockOperator {
public:
    ProbeScanMockOp() : numTuples(2) { generateDataChunks(); }

    void getNextTuples() override;

private:
    void generateDataChunks();

    uint64_t numTuples;
};

class ScanMockOpWithSelector : public DataChunksMockOperator {
public:
    ScanMockOpWithSelector() : numTuples(0) { generateDataChunks(); }

    void getNextTuples() override;

private:
    void generateDataChunks();

    uint64_t numTuples;
};

class ProbeScanMockOpWithSelector : public DataChunksMockOperator {
public:
    ProbeScanMockOpWithSelector() : numTuples(0) { generateDataChunks(); }

    void getNextTuples() override;

private:
    void generateDataChunks();

    uint64_t numTuples;
};

} // namespace processor
} // namespace graphflow
