#pragma once

#include <string>
#include <unordered_map>

#include "src/common/include/assert.h"
#include "src/planner/include/logical_plan/schema.h"
#include "src/processor/include/physical_plan/data_pos.h"

using namespace graphflow::planner;
using namespace std;

namespace graphflow {
namespace processor {

class DataChunkDescriptor {

public:
    DataChunkDescriptor() : isFlat{false} {};

    DataChunkDescriptor(const DataChunkDescriptor& other)
        : expressionNameToValueVectorPosMap{other.expressionNameToValueVectorPosMap},
          expressionNames{other.expressionNames}, isFlat{other.isFlat} {}

    inline uint32_t getValueVectorPos(const string& name) const {
        assert(expressionNameToValueVectorPosMap.contains(name));
        return expressionNameToValueVectorPosMap.at(name);
    }

    inline uint32_t getNumValueVectors() const { return expressionNames.size(); }

    inline void addExpressionName(const string& name) {
        expressionNameToValueVectorPosMap.insert({name, expressionNames.size()});
        expressionNames.push_back(name);
    }

    inline void flatten() { isFlat = true; }

    inline bool getIsFlat() const { return isFlat; }

private:
    unordered_map<string, uint32_t> expressionNameToValueVectorPosMap;
    vector<string> expressionNames;

    bool isFlat;
};

class ResultSetDescriptor {

public:
    explicit ResultSetDescriptor(const Schema& schema);

    ResultSetDescriptor(const ResultSetDescriptor& other);

    DataPos getDataPos(const string& name) const;

    inline uint32_t getNumDataChunks() const { return dataChunkDescriptors.size(); }

    inline DataChunkDescriptor* getDataChunkDescriptor(uint32_t pos) const {
        return dataChunkDescriptors[pos].get();
    }

    inline bool isDataChunkFlat(uint32_t pos) const {
        return dataChunkDescriptors[pos]->getIsFlat();
    }

    inline unique_ptr<ResultSetDescriptor> copy() const {
        return make_unique<ResultSetDescriptor>(*this);
    }

private:
    // Map each variable to its position pair (dataChunkPos, vectorPos)
    unordered_map<string, uint32_t> expressionNameToDataChunkPosMap;
    vector<unique_ptr<DataChunkDescriptor>> dataChunkDescriptors;
};

} // namespace processor
} // namespace graphflow
