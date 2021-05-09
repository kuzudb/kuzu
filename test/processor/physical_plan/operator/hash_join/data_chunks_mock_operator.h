#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/result/result_set_iterator.h"

using namespace graphflow::processor;

namespace graphflow {
namespace processor {

class DataChunksMockOperator : public PhysicalOperator {
public:
    DataChunksMockOperator(MorselsDesc& morsel) : PhysicalOperator(SCAN), morsel(morsel) {
        resultSet = make_shared<ResultSet>();
    }
    explicit DataChunksMockOperator(shared_ptr<ResultSet> mockDataChunks, MorselsDesc& morsel)
        : PhysicalOperator(SCAN), morsel(morsel) {
        resultSet = move(mockDataChunks);
    }

    void getNextTuples() override {}
    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<DataChunksMockOperator>(resultSet, morsel);
    }

    MorselsDesc& morsel;
    mutex opLock;
};

class ScanMockOp : public DataChunksMockOperator {
public:
    ScanMockOp(MorselsDesc& morsel) : DataChunksMockOperator(morsel) { generateDataChunks(); }

    void getNextTuples() override;
    unique_ptr<PhysicalOperator> clone() override { return make_unique<ScanMockOp>(morsel); }

private:
    void generateDataChunks();
};

class ProbeScanMockOp : public DataChunksMockOperator {
public:
    ProbeScanMockOp(MorselsDesc& morsel) : DataChunksMockOperator(morsel) { generateDataChunks(); }

    void getNextTuples() override;
    unique_ptr<PhysicalOperator> clone() override { return make_unique<ProbeScanMockOp>(morsel); }

private:
    void generateDataChunks();
};

class ScanMockOpWithSelector : public DataChunksMockOperator {
public:
    ScanMockOpWithSelector(MorselsDesc& morsel) : DataChunksMockOperator(morsel) {
        generateDataChunks();
    }

    void getNextTuples() override;
    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanMockOpWithSelector>(morsel);
    }

private:
    void generateDataChunks();
};

class ProbeScanMockOpWithSelector : public DataChunksMockOperator {
public:
    ProbeScanMockOpWithSelector(MorselsDesc& morsel) : DataChunksMockOperator(morsel) {
        generateDataChunks();
    }

    void getNextTuples() override;
    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ProbeScanMockOpWithSelector>(morsel);
    }

private:
    void generateDataChunks();
};

} // namespace processor
} // namespace graphflow
