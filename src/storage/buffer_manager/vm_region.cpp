#include "storage/buffer_manager/vm_region.h"

#ifdef _WIN32
// FIXME: should be set by cmake
// Ninja doesn't seem to support CMAKE_GENERATOR_PLATFORM=x64
#define _AMD64_
#include <memoryapi.h>
#include <handleapi.h>
#include <errhandlingapi.h>
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
    DWORD low = (DWORD)(getMaxRegionSize() & 0xFFFFFFFFL);
    DWORD high = (DWORD)((getMaxRegionSize() >> 32) & 0xFFFFFFFFL);
    // SEC_RESERVE lets the virtual memory addresses be reserved without memory or page file space being comitted
    // Unlike with mmap, it's necessary to call VirtualAlloc to commit memory in this address space before it can be used.
    auto handle = CreateFileMappingW(INVALID_HANDLE_VALUE, NULL, PAGE_READWRITE | SEC_RESERVE, high, low, NULL);
    if (handle == NULL) {
        throw BufferManagerException(StringUtils::string_format(
            "CreateFileMapping for size {} failed with error code {}: {}.", getMaxRegionSize(), GetLastError(),
            std::system_category().message(GetLastError())
        ));
    }

    region = (uint8_t *)MapViewOfFile(handle, FILE_MAP_READ | FILE_MAP_WRITE, 0, 0, getMaxRegionSize());
    CloseHandle(handle);
    if (region == NULL) {
        throw BufferManagerException(StringUtils::string_format(
            "MapViewOfFile for size {} failed with error code {}: {}.", getMaxRegionSize(), GetLastError(),
            std::system_category().message(GetLastError())
        ));
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
    VirtualAlloc(region + ((std::uint64_t)frameIdx * frameSize), frameSize, MEM_COMMIT, PAGE_READWRITE);
    return region + ((std::uint64_t)frameIdx * frameSize);
}
#endif

VMRegion::~VMRegion() {
    #ifdef _WIN32
    UnmapViewOfFile(region);
    #else
    munmap(region, getMaxRegionSize());
    #endif
}

void VMRegion::releaseFrame(common::frame_idx_t frameIdx) {
#ifdef _WIN32
    // TODO: There's also DiscardVirtualMemory (windows 8+).
    // Not sure what the differences are
    int error = VirtualUnlock(getFrame(frameIdx), frameSize);
#else
    int error = madvise(getFrame(frameIdx), frameSize, MADV_DONTNEED);
#endif
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
