cmake_minimum_required(VERSION 3.10)
project(diskengine C)

set(CMAKE_C_STANDARD 11)

include_directories(${CMAKE_SOURCE_DIR}/src)

add_library(diskengine STATIC
        src/common/crc32_ieee.c
        src/superblock/superblock.c
        src/wal/wal.c
        src/wal/wal_apply_range.c
        src/alloc/range_alloc.c
        src/blob/blob_record.c
        src/blob/blob_index.c
        src/engine/disk_engine.c
)

add_executable(demo_engine demo/demo_engine.c)
target_link_libraries(demo_engine diskengine)

add_executable(demo_kv demo/demo_kv.c)
target_link_libraries(demo_kv diskengine)
