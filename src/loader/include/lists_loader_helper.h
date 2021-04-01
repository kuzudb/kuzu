#pragma once

#include "src/storage/include/structures/lists_aux_structures.h"

using namespace graphflow::storage;

namespace graphflow {
namespace loader {

typedef vector<atomic<uint64_t>> listSizes_t;

class ListsLoaderHelper {

public:
    static inline void incrementListSize(listSizes_t& listSizes, uint32_t offset, uint32_t val) {
        listSizes[offset].fetch_add(val, memory_order_relaxed);
    }

    static inline uint64_t decrementListSize(
        listSizes_t& listSizes, uint32_t offset, uint32_t val) {
        return listSizes[offset].fetch_sub(val, memory_order_relaxed);
    }

    // Inits (in listHeaders) the header of each list in a Lists structure, from the listSizes.
    // ListSizes is used to determine if the list is small or large, based on which, information is
    // encoded in the 4 byte header.
    static void calculateListHeadersTask(node_offset_t numNodeOffsets, uint32_t numPerPage,
        listSizes_t* listSizes, ListHeaders* listHeaders, shared_ptr<spdlog::logger> logger);

    // Initializes Metadata information of a Lists structure, that is chunksPagesMap and
    // largeListsPagesMap, using listSizes and listHeaders.
    static void calculateListsMetadataTask(uint64_t numNodeOffsets, uint32_t numPerPage,
        listSizes_t* listSizes, ListHeaders* listHeaders, ListsMetadata* listsMetadata,
        shared_ptr<spdlog::logger> logger);

    // Calculates the page idx and offset in page where the data of a particular list has to be put
    // in the in-mem pages.
    static void calculatePageCursor(const uint32_t& header, const uint64_t& reversePos,
        const uint8_t& numBytesPerElement, const node_offset_t& nodeOffset, PageCursor& cursor,
        ListsMetadata& metadata);
};

} // namespace loader
} // namespace graphflow
