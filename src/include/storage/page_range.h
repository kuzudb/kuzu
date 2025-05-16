#pragma once

#include "common/types/types.h"

namespace kuzu::storage {

struct PageRange {
    PageRange() = default;
    PageRange(common::page_idx_t startPageIdx, common::page_idx_t numPages)
        : startPageIdx(startPageIdx), numPages(numPages) {}

    PageRange subrange(common::page_idx_t newStartPage) const {
        KU_ASSERT(newStartPage <= numPages);
        return {startPageIdx + newStartPage, numPages - newStartPage};
    }

    common::page_idx_t startPageIdx;
    common::page_idx_t numPages;
};
} // namespace kuzu::storage
