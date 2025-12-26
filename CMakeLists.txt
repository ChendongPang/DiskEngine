cmake_minimum_required(VERSION 3.16)
project(DiskEngine C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

add_library(diskengine STATIC
        src/common/crc32_ieee.c
        src/superblock/superblock.c
        src/wal/wal.c
        src/wal/wal_apply_range.c
        src/alloc/range_alloc.c
)

target_include_directories(diskengine PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_compile_options(diskengine PRIVATE
        -Wall -Wextra -Wpedantic
)

add_executable(demo_engine
        demo/demo_engine.c
)

target_link_libraries(demo_engine PRIVATE diskengine)
target_compile_options(demo_engine PRIVATE
        -Wall -Wextra -Wpedantic
)
