cmake_minimum_required(VERSION 3.16)
project(SwClock VERSION 1.0.0 LANGUAGES C CXX)

# Set C standard
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Set default build type to Release if not specified
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Configure compiler flags for different build types
set(CMAKE_C_FLAGS_DEBUG "-g -O0 -Wall -Wextra -DDEBUG")
set(CMAKE_C_FLAGS_RELEASE "-O3 -DNDEBUG")

# Platform-specific settings
if(APPLE)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D__APPLE__")
else()
    # Linux: expose POSIX/GNU extensions (clockid_t, pthread_rwlock_t, CLOCK_* etc.)
    add_compile_definitions(_GNU_SOURCE)
endif()

# Source files
set(SWCLOCK_SOURCES
    src/sw_clock/sw_clock.c
    src/sw_clock/sw_clock_utilities.c
    src/sw_clock/sw_clock_structured_log.c
    src/sw_clock/sw_clock_events.c
    src/sw_clock/sw_clock_ringbuf.c
    src/sw_clock/sw_clock_monitor.c
    src/sw_clock/swclock_jsonld.c
    src/sw_clock/sw_clock_commercial_log.c
)

set(SWCLOCK_HEADERS
    src/sw_clock/sw_clock.h
    src/sw_clock/sw_clock_structured_log.h
    src/sw_clock/sw_clock_events.h
    src/sw_clock/sw_clock_ringbuf.h
    src/sw_clock/sw_clock_monitor.h
    src/sw_clock/swclock_jsonld.h
    src/sw_clock/sw_clock_commercial_log.h
)


# Create the main SwClock library
add_library(swclock STATIC ${SWCLOCK_SOURCES} ${SWCLOCK_HEADERS})


# Include directories
target_include_directories(swclock PUBLIC src/sw_clock)

# Link with required libraries
find_library(MATH_LIBRARY m)
if(MATH_LIBRARY)
    target_link_libraries(swclock ${MATH_LIBRARY})
endif()

# Link zlib for JSON-LD log compression
find_package(ZLIB REQUIRED)
target_link_libraries(swclock ZLIB::ZLIB)


# Enable testing and find GTest
include(CTest)
enable_testing()

# Set the path for Homebrew on Apple Silicon and Intel Macs
if(APPLE)
    list(APPEND CMAKE_PREFIX_PATH /opt/homebrew /usr/local)
endif()

find_package(GTest QUIET)

# Look for gtest source files
file(GLOB GTEST_SOURCES "src-gtests/*.cpp" "src-gtests/*.c")

if(GTest_FOUND AND GTEST_SOURCES)
    # Create gtests executable using modern CMake GTest targets
    add_executable(swclock_gtests ${GTEST_SOURCES})
    # Only link gtest_main which automatically includes gtest
    target_link_libraries(swclock_gtests PRIVATE swclock GTest::gtest_main)
    target_include_directories(swclock_gtests PRIVATE src/sw_clock src-gtests)

    # Ensure logs directory exists for test output in project root
    add_custom_command(TARGET swclock_gtests POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_SOURCE_DIR}/logs
        COMMENT "Creating logs directory for test output in project root"
    )

    # Add the test to CTest
    add_test(NAME SwClockGTests COMMAND swclock_gtests)
    message(STATUS "GoogleTest found - unit tests enabled")
elseif(GTEST_SOURCES)
    message(WARNING "GoogleTest not found. Install GoogleTest to build unit tests:")
    message(WARNING "  brew install googletest")
    message(STATUS "Run: ./scripts/setup.sh to install dependencies")
else()
    message(STATUS "No gtest source files found in src-gtests/ directory")
endif()

# Build swclock_event_dump and monitor_demo tools (not applicable on iOS)
if(NOT CMAKE_SYSTEM_NAME STREQUAL "iOS")
    add_executable(swclock_event_dump src-tools/swclock_event_dump.c)
    target_link_libraries(swclock_event_dump PRIVATE swclock)
    target_include_directories(swclock_event_dump PRIVATE src/sw_clock)

    # Build monitor_demo tool (Recommendation 7)
    add_executable(monitor_demo src-tools/monitor_demo.c)
    target_link_libraries(monitor_demo PRIVATE swclock)
    target_include_directories(monitor_demo PRIVATE src/sw_clock)
endif()

# Installation rules
# Skip tool installation for iOS (not applicable)
if(NOT CMAKE_SYSTEM_NAME STREQUAL "iOS")
    install(TARGETS swclock swclock_event_dump monitor_demo
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        RUNTIME DESTINATION bin
        BUNDLE DESTINATION bin
    )
else()
    # For iOS, only install the library (not the tools)
    install(TARGETS swclock
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
    )
endif()

install(FILES ${SWCLOCK_HEADERS}
    DESTINATION include
)

# Print build configuration
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "C compiler: ${CMAKE_C_COMPILER}")
message(STATUS "C flags: ${CMAKE_C_FLAGS}")
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    message(STATUS "Debug flags: ${CMAKE_C_FLAGS_DEBUG}")
elseif(CMAKE_BUILD_TYPE STREQUAL "Release")
    message(STATUS "Release flags: ${CMAKE_C_FLAGS_RELEASE}")
endif()
