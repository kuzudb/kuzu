#pragma once

#include <stdint.h>

#include "robin_hood.h"

#include "src/common/include/types.h"
#include "src/common/include/utils.h"

using namespace graphflow::common;

namespace graphflow {
namespace loader {

// Maps the primary key of a node to the in-system used node offset.
class NodeIDMap {

public:
    NodeIDMap(const uint64_t& size) : size{size}, offsetToNodeIDMap(make_unique<char*[]>(size)){};
    ~NodeIDMap();

    void set(const char* nodeID, node_offset_t nodeOffset);

    node_offset_t get(const char* nodeID);

    inline void createNodeIDToOffsetMap() {
        nodeIDToOffsetMap.reserve(1.5 * size);
        for (auto i = 0u; i < size; i++) {
            try {
                nodeIDToOffsetMap.emplace(offsetToNodeIDMap[i], i);
            } catch (exception& e) { std::rethrow_exception(current_exception()); }
        }
    }

private:
    uint64_t size;
    robin_hood::unordered_flat_map<const char*, node_offset_t, charArrayHasher, charArrayEqualTo>
        nodeIDToOffsetMap;
    unique_ptr<char*[]> offsetToNodeIDMap;
};

} // namespace loader
} // namespace graphflow
