#pragma once

#include <string>
#include <unordered_map>

#include "common/assert.h"
#include "planner/logical_plan/logical_operator/schema.h"
#include "processor/data_pos.h"

using namespace kuzu::planner;
using namespace std;

namespace kuzu {
namespace processor {

class DataChunkDescriptor {

public:
    DataChunkDescriptor() = default;

    DataChunkDescriptor(const DataChunkDescriptor& other)
        : expressionNameToValueVectorPosMap{other.expressionNameToValueVectorPosMap},
          expressionNames{other.expressionNames} {}

    inline uint32_t getValueVectorPos(const string& name) const {
        assert(expressionNameToValueVectorPosMap.contains(name));
        return expressionNameToValueVectorPosMap.at(name);
    }

    inline uint32_t getNumValueVectors() const { return expressionNames.size(); }

    inline void addExpressionName(const string& name) {
        expressionNameToValueVectorPosMap.insert({name, expressionNames.size()});
        expressionNames.push_back(name);
    }

private:
    unordered_map<string, uint32_t> expressionNameToValueVectorPosMap;
    vector<string> expressionNames;
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

    inline unique_ptr<ResultSetDescriptor> copy() const {
        return make_unique<ResultSetDescriptor>(*this);
    }

private:
    // Map each variable to its position pair (dataChunkPos, vectorPos)
    unordered_map<string, uint32_t> expressionNameToDataChunkPosMap;
    vector<unique_ptr<DataChunkDescriptor>> dataChunkDescriptors;
};

} // namespace processor
} // namespace kuzu
