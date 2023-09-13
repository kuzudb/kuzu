// Copyright 2011-2023 David Robillard <d@drobilla.net>
// SPDX-License-Identifier: ISC

#ifndef SERD_SRC_SYSTEM_H
#define SERD_SRC_SYSTEM_H

#include "attributes.h"

#include <stdio.h>

/// Open a file configured for fast sequential reading
FILE*
serd_fopen(const char* path, const char* mode);

/// Allocate a buffer aligned to `alignment` bytes
SERD_MALLOC_FUNC void*
serd_malloc_aligned(size_t alignment, size_t size);

/// Allocate an aligned buffer for I/O
SERD_MALLOC_FUNC void*
serd_allocate_buffer(size_t size);

/// Free a buffer allocated with an aligned allocation function
void
serd_free_aligned(void* ptr);

#endif // SERD_SRC_SYSTEM_H
