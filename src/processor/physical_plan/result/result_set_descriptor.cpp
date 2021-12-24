#include "src/processor/include/physical_plan/result/result_set_descriptor.h"

namespace graphflow {
namespace processor {

ResultSetDescriptor::ResultSetDescriptor(const Schema& schema) {
    for (auto& group : schema.groups) {
        auto dataChunkDescriptor = make_unique<DataChunkDescriptor>();
        for (auto& expressionName : group->expressionNames) {
            expressionNameToDataChunkPosMap.insert({expressionName, dataChunkDescriptors.size()});
            dataChunkDescriptor->addExpressionName(expressionName);
        }
        dataChunkDescriptors.push_back(move(dataChunkDescriptor));
    }
}

ResultSetDescriptor::ResultSetDescriptor(const ResultSetDescriptor& other)
    : expressionNameToDataChunkPosMap{other.expressionNameToDataChunkPosMap} {
    for (auto& dataChunkDescriptor : other.dataChunkDescriptors) {
        dataChunkDescriptors.push_back(make_unique<DataChunkDescriptor>(*dataChunkDescriptor));
    }
}

DataPos ResultSetDescriptor::getDataPos(const string& name) const {
    assert(expressionNameToDataChunkPosMap.contains(name));
    auto dataChunkPos = expressionNameToDataChunkPosMap.at(name);
    auto valueVectorPos = dataChunkDescriptors[dataChunkPos]->getValueVectorPos(name);
    return DataPos{dataChunkPos, valueVectorPos};
}

} // namespace processor
} // namespace graphflow
