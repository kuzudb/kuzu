#ifndef GRAPHFLOW_STORAGE_CATALOG_H_
#define GRAPHFLOW_STORAGE_CATALOG_H_

#include <memory>
#include <unordered_map>
#include <vector>

#include "src/common/include/types.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

class Catalog {

    std::unique_ptr<std::unordered_map<std::string, gfLabel_t>> stringToNodeLabelMap;
    std::unique_ptr<std::unordered_map<std::string, gfLabel_t>> stringToRelLabelMap;
    std::unique_ptr<std::vector<std::unique_ptr<std::vector<Property>>>> nodePropertyMap;
    std::unique_ptr<std::vector<std::unique_ptr<std::vector<Property>>>> relPropertyMap;

public:
    inline uint32_t getNumNodeLabels() { return stringToNodeLabelMap->size(); }
    inline uint32_t getNumRelLabels() { return stringToRelLabelMap->size(); }

    inline void setStringToNodeLabelMap(
        std::unique_ptr<std::unordered_map<std::string, gfLabel_t>> stringToNodeLabelMap) {
        this->stringToNodeLabelMap = std::move(stringToNodeLabelMap);
    }

    inline void setStringToRelLabelMap(
        std::unique_ptr<std::unordered_map<std::string, gfLabel_t>> stringToRelLabelMap) {
        this->stringToRelLabelMap = std::move(stringToRelLabelMap);
    }

    inline void setNodePropertyMap(
        std::unique_ptr<std::vector<std::unique_ptr<std::vector<Property>>>> nodePropertyMap) {
        this->nodePropertyMap = std::move(nodePropertyMap);
    }

    inline void setRelPropertyMap(
        std::unique_ptr<std::vector<std::unique_ptr<std::vector<Property>>>> relPropertyMap) {
        this->relPropertyMap = std::move(relPropertyMap);
    }
};

} // namespace storage
} // namespace graphflow

#endif // GRAPHFLOW_STORAGE_CATALOG_H_
