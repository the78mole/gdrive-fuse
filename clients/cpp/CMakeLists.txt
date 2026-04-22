cmake_minimum_required(VERSION 3.20)
project(gdrive-fuse VERSION 1.0.0 LANGUAGES CXX)

# ── Version string ──────────────────────────────────────────────────────────
# Priority: -DGDRIVE_FUSE_VERSION=<ver>  >  env GDRIVE_FUSE_VERSION  >  git describe  >  PROJECT_VERSION
if(NOT DEFINED GDRIVE_FUSE_VERSION)
  if(DEFINED ENV{GDRIVE_FUSE_VERSION})
    set(GDRIVE_FUSE_VERSION "$ENV{GDRIVE_FUSE_VERSION}")
  else()
    find_package(Git QUIET)
    if(GIT_FOUND)
      execute_process(
        COMMAND "${GIT_EXECUTABLE}" describe --tags --dirty --abbrev=7
        WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
        OUTPUT_VARIABLE _git_describe
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
      )
    endif()
    # v1.2.3-42-gabcdef[-dirty]  →  1.2.3-loc-42[-dirty]
    if(_git_describe MATCHES "^v?[0-9]+\\.[0-9]+\\.[0-9]+-[0-9]+-g[0-9a-f]+")
      string(REGEX REPLACE "^v?([0-9]+\\.[0-9]+\\.[0-9]+)-([0-9]+)-g[0-9a-f]+(.*)"
             "\\1-loc-\\2\\3" GDRIVE_FUSE_VERSION "${_git_describe}")
    elseif(_git_describe MATCHES "^v?[0-9]+\\.[0-9]+\\.[0-9]+")
      string(REGEX REPLACE "^v" "" GDRIVE_FUSE_VERSION "${_git_describe}")
    else()
      set(GDRIVE_FUSE_VERSION "${PROJECT_VERSION}")
    endif()
  endif()
endif()
message(STATUS "gdrive-fuse version: ${GDRIVE_FUSE_VERSION}")

# Set C++20 standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Enable threading
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# Include FetchContent module for dependency management
include(FetchContent)

# Fetch nlohmann/json
FetchContent_Declare(
    json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.11.3
)
FetchContent_MakeAvailable(json)

# Fetch spdlog
FetchContent_Declare(
    spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG v1.13.0
)
FetchContent_MakeAvailable(spdlog)

# Fetch cpr
FetchContent_Declare(
    cpr
    GIT_REPOSITORY https://github.com/libcpr/cpr.git
    GIT_TAG 1.10.5
)
FetchContent_MakeAvailable(cpr)

# Find FUSE3
find_package(PkgConfig REQUIRED)
pkg_check_modules(FUSE3 REQUIRED fuse3)

# Add include directories
include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${FUSE3_INCLUDE_DIRS})

# Collect source files
set(SOURCES
    src/main.cpp
    src/Auth.cpp
    src/GClient.cpp
    src/FuseOps.cpp
)

set(HEADERS
    include/gdrive_fuse/Auth.hpp
    include/gdrive_fuse/GClient.hpp
    include/gdrive_fuse/FuseOps.hpp
)

# Create executable
add_executable(gdrive-fuse ${SOURCES} ${HEADERS})

# Link libraries
target_link_libraries(gdrive-fuse PRIVATE
    nlohmann_json::nlohmann_json
    spdlog::spdlog
    cpr::cpr
    ${FUSE3_LIBRARIES}
    Threads::Threads
)

# Add compile definitions for FUSE
target_compile_definitions(gdrive-fuse PRIVATE FUSE_USE_VERSION=31)

# Add compiler flags for FUSE
target_compile_options(gdrive-fuse PRIVATE ${FUSE3_CFLAGS_OTHER})

# Link directories for FUSE
target_link_directories(gdrive-fuse PRIVATE ${FUSE3_LIBRARY_DIRS})

# Embed version string
target_compile_definitions(gdrive-fuse PRIVATE
    GDRIVE_FUSE_VERSION="${GDRIVE_FUSE_VERSION}"
)

# Optionally embed CLIENT_ID and CLIENT_SECRET at compile time.
# Set these environment variables during cmake / make to bake them into the binary.
# The binary will still accept --client-id / --client-secret / runtime env vars
# with higher priority than the compiled-in defaults.
if(DEFINED ENV{CLIENT_ID} AND NOT "$ENV{CLIENT_ID}" STREQUAL "")
    target_compile_definitions(gdrive-fuse PRIVATE BUILTIN_CLIENT_ID="$ENV{CLIENT_ID}")
    message(STATUS "Embedding CLIENT_ID into binary")
endif()
if(DEFINED ENV{CLIENT_SECRET} AND NOT "$ENV{CLIENT_SECRET}" STREQUAL "")
    target_compile_definitions(gdrive-fuse PRIVATE BUILTIN_CLIENT_SECRET="$ENV{CLIENT_SECRET}")
    message(STATUS "Embedding CLIENT_SECRET into binary")
endif()

# Enable warnings
target_compile_options(gdrive-fuse PRIVATE
    -Wall
    -Wextra
    -Wpedantic
)
