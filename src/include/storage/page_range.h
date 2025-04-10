#pragma once

#include "common/types/types.h"
namespace kuzu::storage {

struct PageRange {
    PageRange() = default;
    PageRange(common::page_idx_t startPageIdx, common::page_idx_t numPages)
        : startPageIdx(startPageIdx), numPages(numPages) {}
    PageRange(const PageRange& o, common::page_idx_t startPageInOther)
        : PageRange(o.startPageIdx + startPageInOther, o.numPages - startPageInOther) {
        KU_ASSERT(startPageInOther <= o.numPages);
    }

    common::page_idx_t startPageIdx;
    common::page_idx_t numPages;
};
} // namespace kuzu::storage
