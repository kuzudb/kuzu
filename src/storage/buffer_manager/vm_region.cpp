#include "storage/buffer_manager/vm_region.h"

#include <sys/mman.h>

#include "common/exception.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

VMRegion::VMRegion(PageSizeClass pageSizeClass, uint64_t maxRegionSize) : numFrameGroups{0} {
    if (maxRegionSize > (std::size_t)-1) {
        throw BufferManagerException("maxRegionSize is beyond the max available mmap region size.");
    }
    frameSize = pageSizeClass == PageSizeClass::PAGE_4KB ? BufferPoolConstants::PAGE_4KB_SIZE :
                                                           BufferPoolConstants::PAGE_256KB_SIZE;
    auto numBytesForFrameGroup = frameSize * StorageConstants::PAGE_GROUP_SIZE;
    maxNumFrameGroups = (maxRegionSize + numBytesForFrameGroup - 1) / numBytesForFrameGroup;
    // Create a private anonymous mapping. The mapping is not shared with other processes and not
    // backed by any file, and its content are initialized to zero.
    region = (uint8_t*)mmap(NULL, getMaxRegionSize(), PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1 /* fd */, 0 /* offset */);
    if (region == MAP_FAILED) {
        throw BufferManagerException(
            "Mmap for size " + std::to_string(getMaxRegionSize()) + " failed.");
    }
}

VMRegion::~VMRegion() {
    munmap(region, getMaxRegionSize());
}

void VMRegion::releaseFrame(common::frame_idx_t frameIdx) {
    int error = madvise(getFrame(frameIdx), frameSize, MADV_DONTNEED);
    if (error != 0) {
        throw BufferManagerException(
            "Releasing physical memory associated with a frame failed with error code " +
            std::to_string(error) + ": " + std::string(std::strerror(errno)));
    }
}

frame_group_idx_t VMRegion::addNewFrameGroup() {
    std::unique_lock xLck{mtx};
    if (numFrameGroups >= maxNumFrameGroups) {
        throw BufferManagerException("No more frame groups can be added to the allocator.");
    }
    return numFrameGroups++;
}

} // namespace storage
} // namespace kuzu
