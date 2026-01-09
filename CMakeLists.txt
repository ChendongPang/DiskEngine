cmake_minimum_required(VERSION 3.10)
project(diskengine C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

find_package(PkgConfig REQUIRED)

# ----------------------------
# SPDK_ROOT
# ----------------------------
if (DEFINED ENV{SPDK_ROOT} AND NOT "$ENV{SPDK_ROOT}" STREQUAL "")
    set(SPDK_ROOT "$ENV{SPDK_ROOT}")
else()
    set(SPDK_ROOT "$ENV{HOME}/workspace/spdk")
endif()

set(SPDK_LIBDIR   "${SPDK_ROOT}/build/lib")
set(SPDK_LIBDIR64 "${SPDK_ROOT}/build/lib64")
set(DPDK_LIBDIR   "${SPDK_ROOT}/dpdk/build/lib")
set(DPDK_LIBDIR64 "${SPDK_ROOT}/dpdk/build/lib64")

# Prefer embedding rpath so sudo-run works without LD_LIBRARY_PATH
set(CMAKE_BUILD_RPATH   "${SPDK_LIBDIR};${SPDK_LIBDIR64};${DPDK_LIBDIR};${DPDK_LIBDIR64}")
set(CMAKE_INSTALL_RPATH "${SPDK_LIBDIR};${SPDK_LIBDIR64};${DPDK_LIBDIR};${DPDK_LIBDIR64}")
set(CMAKE_SKIP_BUILD_RPATH FALSE)
set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)

# Avoid DT_RUNPATH so sudo/ld.so behavior is predictable; keep RPATH
add_link_options(-Wl,--disable-new-dtags)

# ----------------------------
# Add SPDK pkgconfig paths
# ----------------------------
set(_spdk_pc_paths
    "${SPDK_LIBDIR}/pkgconfig"
    "${SPDK_LIBDIR64}/pkgconfig"
)

if (DEFINED ENV{PKG_CONFIG_PATH} AND NOT "$ENV{PKG_CONFIG_PATH}" STREQUAL "")
    set(ENV{PKG_CONFIG_PATH} "${_spdk_pc_paths}:$ENV{PKG_CONFIG_PATH}")
else()
    set(ENV{PKG_CONFIG_PATH} "${_spdk_pc_paths}")
endif()

# ----------------------------
# Helper: query pkg-config flags
# ----------------------------
function(pkgconf_get out_var)
    set(oneValueArgs MODULE WHAT)
    cmake_parse_arguments(P "" "${oneValueArgs}" "" ${ARGN})
    execute_process(
        COMMAND pkg-config --${P_WHAT} ${P_MODULE}
        OUTPUT_VARIABLE _str
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    separate_arguments(_list UNIX_COMMAND "${_str}")
    set(${out_var} ${_list} PARENT_SCOPE)
endfunction()

# ----------------------------
# Helper: remove SPDK build/include from pkg-config cflags
# ----------------------------
function(spdk_sanitize_cflags in_list out_list)
    set(_tmp ${${in_list}})
    list(FILTER _tmp EXCLUDE REGEX "^-I${SPDK_ROOT}/build/include$")
    list(FILTER _tmp EXCLUDE REGEX "^-I${SPDK_ROOT}/build/include/.*")
    list(FILTER _tmp EXCLUDE REGEX "^-isystem${SPDK_ROOT}/build/include$")
    list(FILTER _tmp EXCLUDE REGEX "^-isystem${SPDK_ROOT}/build/include/.*")
    list(FILTER _tmp EXCLUDE REGEX ".*/spdk/build/include.*")
    set(${out_list} ${_tmp} PARENT_SCOPE)
endfunction()

# ----------------------------
# SPDK flags/libs
# ----------------------------
pkgconf_get(SPDK_BDEV_CFLAGS_RAW   MODULE spdk_bdev      WHAT cflags)
pkgconf_get(SPDK_BDEV_LIBS         MODULE spdk_bdev      WHAT libs)
pkgconf_get(SPDK_EVENT_CFLAGS_RAW  MODULE spdk_event     WHAT cflags)
pkgconf_get(SPDK_EVENT_LIBS        MODULE spdk_event     WHAT libs)
pkgconf_get(SPDK_ENV_LIBS          MODULE spdk_env_dpdk  WHAT libs)

spdk_sanitize_cflags(SPDK_BDEV_CFLAGS_RAW  SPDK_BDEV_CFLAGS)
spdk_sanitize_cflags(SPDK_EVENT_CFLAGS_RAW SPDK_EVENT_CFLAGS)

# ----------------------------
# System deps
# ----------------------------
find_library(SSL_LIB    NAMES ssl)
find_library(CRYPTO_LIB NAMES crypto)
find_library(UUID_LIB   NAMES uuid)

if (NOT SSL_LIB OR NOT CRYPTO_LIB)
    message(FATAL_ERROR "OpenSSL libs not found. Please install libssl-dev.")
endif()
if (NOT UUID_LIB)
    message(FATAL_ERROR "uuid lib not found. Please install uuid-dev.")
endif()

# ----------------------------
# ISA-L / ISA-L-crypto
# ----------------------------
set(ISAL_LIBA         "${SPDK_ROOT}/isa-l/.libs/libisal.a")
set(ISAL_CRYPTO_LIBA  "${SPDK_ROOT}/isa-l-crypto/.libs/libisal_crypto.a")

if (NOT EXISTS "${ISAL_LIBA}")
    message(FATAL_ERROR "libisal.a not found at ${ISAL_LIBA}. Build ${SPDK_ROOT}/isa-l first.")
endif()
if (NOT EXISTS "${ISAL_CRYPTO_LIBA}")
    message(FATAL_ERROR "libisal_crypto.a not found at ${ISAL_CRYPTO_LIBA}. Build ${SPDK_ROOT}/isa-l-crypto first.")
endif()

# ----------------------------
# Create an INTERFACE target for SPDK stack
# ----------------------------
option(SPDK_ENABLE_RPATH "Embed rpath to SPDK/DPDK .so dirs" ON)

add_library(spdk_stack INTERFACE)

# ✅ FIX: BEFORE must come BEFORE scope keyword (INTERFACE/PUBLIC/PRIVATE)
target_include_directories(spdk_stack BEFORE INTERFACE
    "${SPDK_ROOT}/include"
)

target_compile_options(spdk_stack INTERFACE
    ${SPDK_BDEV_CFLAGS}
)

if (SPDK_ENABLE_RPATH)
    target_link_options(spdk_stack INTERFACE
        "-Wl,--no-as-needed"
        "-Wl,--export-dynamic"
        "-Wl,-rpath-link,${SPDK_LIBDIR}"
        "-Wl,-rpath-link,${SPDK_LIBDIR64}"
        "-Wl,-rpath-link,${DPDK_LIBDIR}"
        "-Wl,-rpath-link,${DPDK_LIBDIR64}"
        "-Wl,-rpath,${SPDK_LIBDIR}"
        "-Wl,-rpath,${SPDK_LIBDIR64}"
        "-Wl,-rpath,${DPDK_LIBDIR}"
        "-Wl,-rpath,${DPDK_LIBDIR64}"
    )
else()
    target_link_options(spdk_stack INTERFACE
        "-Wl,--no-as-needed"
        "-Wl,--export-dynamic"
    )
endif()

target_link_libraries(spdk_stack INTERFACE
    ${SPDK_BDEV_LIBS}
    ${SPDK_ENV_LIBS}
    ${SSL_LIB} ${CRYPTO_LIB} ${UUID_LIB}
    pthread dl rt m
)

target_link_libraries(spdk_stack INTERFACE
    "-Wl,--whole-archive"
    "${ISAL_CRYPTO_LIBA}"
    "${ISAL_LIBA}"
    "-Wl,--no-whole-archive"
)

# ----------------------------
# diskengine (static)
# ----------------------------
add_library(diskengine STATIC
    src/common/crc32_ieee.c
    src/superblock/superblock.c
    src/wal/wal.c
    src/wal/wal_apply_range.c
    src/alloc/range_alloc.c
    src/blob/blob_record.c
    src/blob/blob_index.c
    src/engine/disk_engine.c
    src/io/io_spdk_backend.c
)

target_include_directories(diskengine PUBLIC
    ${CMAKE_SOURCE_DIR}/src
)

# ✅ FIX: BEFORE position
target_include_directories(diskengine BEFORE PRIVATE
    "${SPDK_ROOT}/include"
)

target_compile_options(diskengine PRIVATE
    ${SPDK_BDEV_CFLAGS}
)

# KV layer
add_library(kvstore STATIC src/kv/kv_store.c)
target_include_directories(kvstore PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(kvstore PRIVATE diskengine)

# ----------------------------
# demos
# ----------------------------
add_executable(demo_engine demo/demo_engine.c)
target_link_libraries(demo_engine PRIVATE diskengine spdk_stack)

add_executable(demo_kv demo/demo_kv.c)
target_link_libraries(demo_kv PRIVATE kvstore spdk_stack)

add_executable(demo_kv2 demo/demo_kv2.c)
target_link_libraries(demo_kv2 PRIVATE kvstore spdk_stack)

add_executable(demo_kv_spdk demo/demo_kv_spdk.c)
target_link_libraries(demo_kv_spdk PRIVATE diskengine spdk_stack)

# event-specific flags/libs only for this executable
# ✅ FIX: BEFORE position
target_include_directories(demo_kv_spdk BEFORE PRIVATE
    "${SPDK_ROOT}/include"
)
target_compile_options(demo_kv_spdk PRIVATE ${SPDK_EVENT_CFLAGS})
target_link_libraries(demo_kv_spdk PRIVATE ${SPDK_EVENT_LIBS})
