cmake_minimum_required(VERSION 3.20)
project(gdrive-fuse VERSION 1.0.0 LANGUAGES CXX)

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

# Enable warnings
target_compile_options(gdrive-fuse PRIVATE
    -Wall
    -Wextra
    -Wpedantic
)
