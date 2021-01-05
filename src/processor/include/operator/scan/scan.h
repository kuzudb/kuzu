#pragma once

#include <memory>

#include "src/common/include/file_ser_deser_helper.h"
#include "src/common/include/vector/node_vector.h"
#include "src/processor/include/operator/operator.h"

namespace graphflow {
namespace processor {

class Scan : public Operator {

public:
    Scan(string variableName) : variableName(variableName){};
    Scan(FileDeserHelper& fdsh);

    void initialize(Graph* graph) override;

    void getNextTuples() override = 0;

    void cleanup() override{};

    void serialize(FileSerHelper& fsh) override;

    shared_ptr<NodeIDSequenceVector>& getNodeVector() { return nodeIDVector; }

    const string& getVariableName() { return variableName; }

protected:
    const string variableName;
    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<NodeIDSequenceVector> nodeIDVector;
    node_offset_t currMorselNodeOffset{0};
};

class ScanSingleLabel : public Scan {

public:
    ScanSingleLabel(string variableName, shared_ptr<MorselDescSingleLabelNodeIDs>& morsel)
        : Scan{variableName}, morsel{morsel} {};
    ScanSingleLabel(FileDeserHelper& fdsh);

    void initialize(Graph* graph) override;

    void getNextTuples() override;

    bool hasNextMorsel() override;

    unique_ptr<Operator> clone() override {
        return make_unique<ScanSingleLabel>(variableName, morsel);
    }

    void serialize(FileSerHelper& fsh) override;

protected:
    shared_ptr<MorselDescSingleLabelNodeIDs> morsel;
};

class ScanMultiLabel : public Scan {

public:
    ScanMultiLabel(string variableName, shared_ptr<MorselDescMultiLabelNodeIDs>& morsel)
        : Scan{variableName}, morsel{morsel} {};
    ScanMultiLabel(FileDeserHelper& fdsh);

    void getNextTuples() override;

    bool hasNextMorsel() override;

    unique_ptr<Operator> clone() override {
        return make_unique<ScanMultiLabel>(variableName, morsel);
    }

    void serialize(FileSerHelper& fsh) override;

protected:
    shared_ptr<MorselDescMultiLabelNodeIDs> morsel;
    uint64_t currMorselLabelPos{0};
};

} // namespace processor
} // namespace graphflow
