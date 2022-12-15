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
        : singleState{other.singleState},
          expressionNameToValueVectorPosMap{other.expressionNameToValueVectorPosMap},
          expressions{other.expressions} {}
    ~DataChunkDescriptor() = default;

    inline void setSingleState() { singleState = true; }
    inline bool isSingleState() const { return singleState; }

    inline uint32_t getNumValueVectors() const { return expressions.size(); }

    inline void addExpression(shared_ptr<Expression> expression) {
        expressionNameToValueVectorPosMap.insert({expression->getUniqueName(), expressions.size()});
        expressions.push_back(std::move(expression));
    }
    inline shared_ptr<Expression> getExpression(uint32_t idx) const { return expressions[idx]; }

private:
    bool singleState = false;
    unordered_map<string, uint32_t> expressionNameToValueVectorPosMap;
    expression_vector expressions;
};

class ResultSetDescriptor {
public:
    explicit ResultSetDescriptor(const Schema& schema);
    ResultSetDescriptor(const ResultSetDescriptor& other);
    ~ResultSetDescriptor() = default;

    inline uint32_t getNumDataChunks() const { return dataChunkDescriptors.size(); }

    inline DataChunkDescriptor* getDataChunkDescriptor(uint32_t pos) const {
        return dataChunkDescriptors[pos].get();
    }

    inline unique_ptr<ResultSetDescriptor> copy() const {
        return make_unique<ResultSetDescriptor>(*this);
    }

private:
    unordered_map<string, uint32_t> expressionNameToDataChunkPosMap;
    vector<unique_ptr<DataChunkDescriptor>> dataChunkDescriptors;
};

} // namespace processor
} // namespace kuzu
