#include "storage/buffer_manager/vm_region.h"

#include "common/string_utils.h"

#ifdef _WIN32
#include <errhandlingapi.h>
#include <handleapi.h>
#include <memoryapi.h>
#else
#include <sys/mman.h>
#endif

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
#ifdef _WIN32
    region = (uint8_t*)VirtualAlloc(NULL, getMaxRegionSize(), MEM_RESERVE, PAGE_READWRITE);
    if (region == NULL) {
        throw BufferManagerException(StringUtils::string_format(
            "VirtualAlloc for size {} failed with error code {}: {}.", getMaxRegionSize(),
            GetLastError(), std::system_category().message(GetLastError())));
    }
#else
    // Create a private anonymous mapping. The mapping is not shared with other processes and not
    // backed by any file, and its content are initialized to zero.
    region = (uint8_t*)mmap(NULL, getMaxRegionSize(), PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1 /* fd */, 0 /* offset */);
    if (region == MAP_FAILED) {
        throw BufferManagerException(
            "Mmap for size " + std::to_string(getMaxRegionSize()) + " failed.");
    }
#endif
}

#ifdef _WIN32
uint8_t* VMRegion::getFrame(common::frame_idx_t frameIdx) {
    auto result = VirtualAlloc(
        region + ((std::uint64_t)frameIdx * frameSize), frameSize, MEM_COMMIT, PAGE_READWRITE);
    if (result == NULL) {
        throw BufferManagerException(StringUtils::string_format(
            "VirtualAlloc MEM_COMMIT failed with error code {}: {}.", getMaxRegionSize(),
            GetLastError(), std::system_category().message(GetLastError())));
    }
    return region + ((std::uint64_t)frameIdx * frameSize);
}
#endif

VMRegion::~VMRegion() {
#ifdef _WIN32
    VirtualFree(region, 0, MEM_RELEASE);
#else
    munmap(region, getMaxRegionSize());
#endif
}

void VMRegion::releaseFrame(common::frame_idx_t frameIdx) {
#ifdef _WIN32
    // TODO: VirtualAlloc(..., MEM_RESET, ...) may be faster
    // See https://arvid.io/2018/04/02/memory-mapping-on-windows/#1
    // Not sure what the differences are
    if (!VirtualFree(getFrame(frameIdx), frameSize, MEM_DECOMMIT)) {
        throw BufferManagerException(StringUtils::string_format(
            "Releasing physical memory associated with a frame failed with error code {}: {}.",
            GetLastError(), std::system_category().message(GetLastError())));
    }

#else
    int error = madvise(getFrame(frameIdx), frameSize, MADV_DONTNEED);
    if (error != 0) {
        throw BufferManagerException(
            "Releasing physical memory associated with a frame failed with error code " +
            std::to_string(error) + ": " + std::string(std::strerror(errno)));
    }
#endif
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
