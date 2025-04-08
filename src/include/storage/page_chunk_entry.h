#pragma once

#include "common/types/types.h"
namespace kuzu::storage {

struct PageChunkEntry {
    PageChunkEntry() = default;
    PageChunkEntry(common::page_idx_t startPageIdx, common::page_idx_t numPages)
        : startPageIdx(startPageIdx), numPages(numPages) {}
    PageChunkEntry(const PageChunkEntry& o, common::page_idx_t startPageInOther)
        : PageChunkEntry(o.startPageIdx + startPageInOther, o.numPages - startPageInOther) {
        KU_ASSERT(startPageInOther <= o.numPages);
    }

    common::page_idx_t startPageIdx;
    common::page_idx_t numPages;
};
} // namespace kuzu::storage
