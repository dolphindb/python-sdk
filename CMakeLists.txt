cmake_minimum_required(VERSION 3.15.1)
project(dolphindbcpp LANGUAGES CXX)

set(MODULE_NAME _dolphindbcpp)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")



# ============================================================================
# Compiler Flags
# ============================================================================
if(MSVC)
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /O2 /d2FH4-")
    set(CMAKE_CXX_FLAGS_DEBUG   "${CMAKE_CXX_FLAGS_DEBUG} /Zi /Od")
else()
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")
    set(CMAKE_CXX_FLAGS_DEBUG   "${CMAKE_CXX_FLAGS_DEBUG} -g -O0")
endif()


# ============================================================================
# Dependencies: OpenSSL
# ============================================================================

# Use OPENSSL_ROOT_DIR if provided
if(DEFINED ENV{OPENSSL_ROOT_DIR})
    set(OPENSSL_ROOT_DIR "$ENV{OPENSSL_ROOT_DIR}" CACHE PATH "OpenSSL root directory")
    message(STATUS "Using OPENSSL_ROOT_DIR from environment: ${OPENSSL_ROOT_DIR}")

# Otherwise, use Conda's environment if available
elseif(DEFINED ENV{CONDA_PREFIX})
    set(OPENSSL_ROOT_DIR "$ENV{CONDA_PREFIX}" CACHE PATH "OpenSSL root directory from Conda")
    message(STATUS "Using OpenSSL from Conda environment: ${OPENSSL_ROOT_DIR}")
endif()

# Always set CMAKE_PREFIX_PATH to help find_package()
if(DEFINED OPENSSL_ROOT_DIR)
    list(APPEND CMAKE_PREFIX_PATH "${OPENSSL_ROOT_DIR}")
endif()

if(MSVC)
set(OPENSSL_MSVC_STATIC_RT TRUE)
endif()

find_package(OpenSSL 1.0.2 REQUIRED)
message(STATUS "OpenSSL version: ${OPENSSL_VERSION}")
message(STATUS "OpenSSL root dir: ${OPENSSL_ROOT_DIR}")

set(OPENSSL_USE_STATIC_LIBS TRUE)


# ============================================================================
# Dependencies: Python + pybind11
# ============================================================================
if(WIN32)
    if (CMAKE_VERSION VERSION_GREATER_EQUAL "3.30")
        set (Python3_FIND_ABI "OFF" "ANY" "OFF" "ANY")
    endif()
    find_package(Python REQUIRED COMPONENTS Interpreter Development)
else()
    execute_process(COMMAND which python3 OUTPUT_VARIABLE PYTHON_PATH OUTPUT_STRIP_TRAILING_WHITESPACE)
    set(Python_EXECUTABLE ${PYTHON_PATH})
endif()

message(STATUS "Python: ${Python_EXECUTABLE}")
message(STATUS "Python include: ${Python_INCLUDE_DIRS}")
message(STATUS "Python libraries: ${Python_LIBRARIES}")


# pybind11 from config mode
execute_process(COMMAND pybind11-config --cmakedir OUTPUT_VARIABLE pybind11_DIR OUTPUT_STRIP_TRAILING_WHITESPACE)
file(TO_CMAKE_PATH "${pybind11_DIR}" pybind11_DIR)
find_package(pybind11 CONFIG REQUIRED)
message(STATUS "pybind11 version: ${pybind11_VERSION}")


# ============================================================================
# Source files
# ============================================================================
file(GLOB_RECURSE SRC core/src/*.cpp)
file(GLOB_RECURSE ABSL_SRC core/thirdparty/abseil/src/*.cpp)
list(APPEND SRC ${ABSL_SRC})


# ============================================================================
# Build pybind11 module
# ============================================================================
pybind11_add_module(${MODULE_NAME} ${SRC})

target_include_directories(${MODULE_NAME}
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/core/include
        ${CMAKE_CURRENT_SOURCE_DIR}/core/thirdparty/abseil/include
        ${CMAKE_CURRENT_SOURCE_DIR}/core/thirdparty/spdlog/include
        ${Python_INCLUDE_DIRS}
)

target_compile_definitions(${MODULE_NAME}
    PUBLIC
        MODULE_NAME=${MODULE_NAME}
        NDEBUG
        LOGGING_LEVEL_1
        _hypot=hypot
)

if(WIN32)
target_include_directories(${MODULE_NAME}
    PRIVATE
        ${OPENSSL_INCLUDE_DIR}
)
target_link_directories(${MODULE_NAME}
    PUBLIC
        ${Python_LIBRARY_DIRS}
        ${OPENSSL_LIBRARIES}
)
endif()

target_compile_features(${MODULE_NAME} PRIVATE cxx_std_17)
target_compile_features(${MODULE_NAME} PRIVATE c_std_99)



# ============================================================================
# Platform-specific settings
# ============================================================================
if(WIN32)
    target_compile_definitions(${MODULE_NAME} PRIVATE WINDOWS)

    if(MINGW)
        set(CMAKE_CXX_STANDARD_LIBRARIES "-static -static-libgcc -static-libstdc++ ${CMAKE_CXX_STANDARD_LIBRARIES}")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-Bstatic,--whole-archive -lwinpthread -Wl,--no-whole-archive")
        target_link_libraries(${MODULE_NAME} PRIVATE ws2_32)
    elseif(MSVC)
        target_compile_definitions(${MODULE_NAME} PUBLIC
            WIN32_LEAN_AND_MEAN _WINSOCK_DEPRECATED_NO_WARNINGS _CRT_SECURE_NO_WARNINGS
            NOMINMAX _WINDOWS _USRDLL FMT_UNICODE=0
        )
        get_filename_component(OPENSSL_ROOT_DIR ${OPENSSL_INCLUDE_DIR} DIRECTORY)
        target_link_directories(${MODULE_NAME}
            PUBLIC
                "${OPENSSL_ROOT_DIR}/lib64"
                "${OPENSSL_ROOT_DIR}/lib"
        )
        target_compile_options(${MODULE_NAME} PRIVATE /MP /Zc:__cplusplus /utf-8)
        target_link_libraries(${MODULE_NAME}
            PRIVATE
                ws2_32
                libcryptoMT
                libsslMT
                advapi32             # needed by openssl static lib
                user32               # needed by openssl static lib
                crypt32              # needed by openssl static lib
        )
    endif()

elseif(APPLE)
    find_package(UUID REQUIRED)

    if(NOT UUID_FOUND)
        message(FATAL_ERROR "UUID_ROOT_DIR not defined. You can install uuid via Homebrew:\n"
            "  brew install util-linux\n"
            "  export UUID_ROOT_DIR=$(brew --prefix util-linux)")
    endif()

    target_compile_definitions(${MODULE_NAME} PUBLIC MAC)
    target_link_libraries(${MODULE_NAME}
        PRIVATE
            OpenSSL::SSL
            OpenSSL::Crypto
            ${Python_LIBRARIES}
    )
    target_link_libraries(${MODULE_NAME} PRIVATE UUID::UUID_STATIC pthread dl)

elseif(UNIX)
    target_compile_options(${MODULE_NAME} PRIVATE -fsigned-char)
    target_compile_definitions(${MODULE_NAME} PRIVATE LINUX GLIBCXX_USE_CXX11_ABI=0)
    target_link_libraries(${MODULE_NAME}
        PRIVATE
            OpenSSL::SSL
            OpenSSL::Crypto
            ${Python_LIBRARIES}
    )
    target_link_libraries(${MODULE_NAME} PRIVATE uuid rt pthread)
endif()


# ============================================================================
# Clean up MSVC flags
# ============================================================================
string(REPLACE "/RTC1" "" CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")