## Stores

Each store is composed of multiple instances of STRUCTURE having a fixed LAYOUT.

- NodePropertyStore
- RelPropertyStore
- AdjListsIndexes

## Structures:

Data structure that we use to hold some data. The **Layout** is fixed by the data it holds.

- Vertex Column **(VCOL)**
- Property Lists **(PROPL)**
- Adjacency Lists **(ADJL)**

Eg, VCOL\<int> is a Vertex Column with layout for storing integers.

## Node Property Store:

Holds properties of nodes in Vertex columns. There is one VCOL for each (node label, property) pair with a fixed layout.
VCOLs can be instances of following types:

- COL\<int>, VCOL\<double>, VCOL\<bool>, VCOL\<str>

## Rel Property Store:

Holds properties of rels in Vertex columns (for rels having single relMultiplicity in any direction) and Property
lists (for MANY-MANY rels).

- PROPL\<int>, PROPL\<double>, PROPL\<bool>, PROPL\<str>
- VCOL\<int>, VCOL\<double>, VCOL\<bool>, VCOL\<str>

## AdjacencyListsIndexes

Holds adjacency lists in the FWD as well as the BWD direction. In each direction, the adjlsts are stored in either
Defualts Adjacency Lists (ADJL) (for relLabels that have MANY relMultiplicity) or in Vertex Columns (VCOL) (for rels
that have single relMultiplicity).

- FWD
    - ADJL\<label, offset>
        - label  : [Optional] [none, 1 byte, 2 byte, 4 byte]
        - offset : [1 byte, 2 byte, 4 byte, 8 byte]
    - COL<label, offset>
        - label  : [Optional] [none, 1 byte, 2 byte, 4 byte]
        - offset : [1 byte, 2 byte, 4 byte, 8 byte]
- BWD
    - ADJL<label, offset, loffest>
        - label  : [Optional] [none, 1 byte, 2 byte, 4 byte]
        - offset : [1 byte, 2 byte, 4 byte, 8 byte]
        - loffset: [Optional] [none, 1 byte, 2 byte, 4 byte]
    - COL<label, offset>
        - label  : [Optional] [none, 1 byte, 2 byte, 4 byte]
        - offset : [1 byte, 2 byte, 4 byte, 8 byte]

## FILE:

Each instance of a STRUCTURE (with definite layout) has a FILE object that controls the storage of its data in a file in
a physical disk.

- `fileDescriptor:int` owns the FileStream.
- `page:uint64_t[]` array of handles, one per populated page.
- recieves request for the page from STRUCTURE and routes it to the BUFFER MANAGER.

## PAGE:

Each file is broken into PAGES (of fixed size of 4096 bytes).

## BUFFER MANAGER:

- `lock` needed for synchronized operations.
- `list:FrameHandles[]` {size: max_size/page_size}.
- `pointer` needed for the CLOCK replacement policy.

## FRAME HANDLE:

Manages a frame in the buffer manager.

- `location:uint64_t *`: holds the page's address corresponding to the data in frame, else null_ptr.
- `unswizzledContent:uint64_t`: content of
- `pinAndRecentlyAccessed:uint8_t`
- `frame:uint8_t[]`