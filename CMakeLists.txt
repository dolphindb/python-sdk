cmake_minimum_required(VERSION 3.15.1) # Cannot be lower than 3.15, otherwise python cannot be found correctly
message("CMAKE VERSION: ${CMAKE_VERSION}")
message("CMAKE PATH: ${CMAKE_COMMAND}")
project(dolphindbcpp)
set(MODULE_NAME _dolphindbcpp)

set(CMAKE_CXX_STANDARD 11)

if(MSVC)
    # set(ENV{PATH} "$ENV{VIRTUAL_ENV}/Scripts;$ENV{PATH}")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_DEBUG} /O2 /d2FH4-")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /Zi /Od")
else()
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0")
endif()

# find openssl
if(DEFINED ENV{OPENSSL_ROOT_DIR})
    set(OPENSSL_ROOT_DIR $ENV{OPENSSL_ROOT_DIR})
endif()
file(TO_CMAKE_PATH ${OPENSSL_ROOT_DIR} OPENSSL_ROOT_DIR)
message(STATUS "[OPENSSL_ROOT_DIR ${OPENSSL_ROOT_DIR}]")
set(OPENSSL_USE_STATIC_LIBS TRUE)
find_package(OpenSSL 1.0.2 REQUIRED)
if(OPENSSL_VERSION VERSION_LESS "1.0.2" OR OPENSSL_VERSION VERSION_GREATER "1.1.1")
    message(ERROR "Need OpenSSL [1.0.2 - 1.1.1], ${OPENSSL_VERSION} is unavailable!")
    message(ERROR "You can add OPENSSL_ROOT_DIR=<openssl> to environment")
endif()
message(STATUS "[openssl ${OPENSSL_VERSION}]")

# find python3
function(which_python PYCMD PYPATH)
    execute_process(COMMAND which ${PYCMD} OUTPUT_VARIABLE P)
    string(REGEX MATCH "^(/.*)\n" P "${P}")
    file(TO_CMAKE_PATH ${CMAKE_MATCH_1} P)
    set(${PYPATH} ${P} PARENT_SCOPE)
endfunction(which_python)

function(python_version PYCMD VA VB VC)
    execute_process(COMMAND ${PYCMD} --version OUTPUT_VARIABLE Python_VERSION)
    string(REGEX MATCH "^Python ([0-9]+)\\.([0-9]+)\\.([0-9]+)\n$" Python_VERSION ${Python_VERSION})
    set(${VA} ${CMAKE_MATCH_1} PARENT_SCOPE)
    set(${VB} ${CMAKE_MATCH_2} PARENT_SCOPE)
    set(${VC} ${CMAKE_MATCH_3} PARENT_SCOPE)
endfunction(python_version)

set(PYCMD python)
python_version(${PYCMD} VA VB VC)
if(VA STREQUAL "2")
    set(PYCMD python3)
    python_version(${PYCMD} VA VB VC)
endif()

if(WIN32)
    find_package(Python ${VA}.${VB} EXACT REQUIRED COMPONENTS Interpreter Development)
    message(STATUS "[python ${Python_VERSION}]")
else()
    which_python(${PYCMD} PYPATH)
    set(PYTHON_EXECUTABLE ${PYPATH})
    message(STATUS "[python executable ${PYPATH}]")
endif()

message(STATUS "[Python Found: ${Python_FOUND}]")
message(STATUS "[Python Include PATH: ${Python_INCLUDE_DIRS}]")
message(STATUS "[Python Executable PATH: ${Python_EXECUTABLE}]")
message(STATUS "[Python Lib PATH: ${Python_LIBRARY}]")

# find pybind11
execute_process(COMMAND pybind11-config --cmakedir OUTPUT_VARIABLE pybind11_DIR)
file(TO_CMAKE_PATH ${pybind11_DIR} pybind11_DIR)
string(REGEX REPLACE "\n$" "" pybind11_DIR ${pybind11_DIR})
find_package(pybind11 CONFIG REQUIRED)
message(STATUS "[pybind11 ${pybind11_VERSION}]")


# add src to pybind11 module
aux_source_directory(core/src SRC)
pybind11_add_module(${MODULE_NAME} ${SRC})

# definitions
target_compile_definitions(${MODULE_NAME}
    PUBLIC MODULE_NAME=${MODULE_NAME}
    PUBLIC NDEBUG
    PUBLIC LOGGING_LEVEL_1
    PUBLIC _hypot=hypot
)

# include-dirs
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/core/include
    ${OPENSSL_INCLUDE_DIR}
    ${Python_INCLUDE_DIRS}
)

# lib-dirs
target_link_directories(${MODULE_NAME}
    PUBLIC ${Python_LIBRARY_DIRS}
)

# lib-files
target_link_libraries(${MODULE_NAME} PUBLIC
    ${OPENSSL_LIBRARIES}
)

if(WIN32)
    # definitions
    target_compile_definitions(${MODULE_NAME}
        PUBLIC WINDOWS
        PUBLIC MS_WIN64
    )

    if(MINGW)
        # mingw static link "libgcc_s_seh-1.dll" "libstdc++-6.dll" "libwinpthread-1.dll"
        set(CMAKE_CXX_STANDARD_LIBRARIES "-static -static-libgcc -static-libstdc++ ${CMAKE_CXX_STANDARD_LIBRARIES}")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-Bstatic,--whole-archive -lwinpthread -Wl,--no-whole-archive")
        message(STATUS "cxx libraries: " ${CMAKE_CXX_STANDARD_LIBRARIES})
        message(STATUS "linker flags: " ${CMAKE_EXE_LINKER_FLAGS})

        target_link_libraries(${MODULE_NAME} PUBLIC
            C:/Windows/System32/ws2_32.dll
        )

    elseif(MSVC)
        target_compile_definitions(${MODULE_NAME}
            PUBLIC WIN32_LEAN_AND_MEAN
            PUBLIC _WINSOCK_DEPRECATED_NO_WARNINGS
            PUBLIC _CRT_SECURE_NO_WARNINGS
            PUBLIC NOMINMAX
            PUBLIC _WINDOWS
            PUBLIC _USRDLL
        )

        get_filename_component(OPENSSL_ROOT_DIR ${OPENSSL_INCLUDE_DIR} DIRECTORY)
        target_link_directories(${MODULE_NAME}
            PUBLIC "${OPENSSL_ROOT_DIR}/lib64"
            PUBLIC "${OPENSSL_ROOT_DIR}/lib"
        )

        target_link_libraries(${MODULE_NAME} PUBLIC
            ws2_32
            libcryptoMT
            libsslMT
            advapi32             # needed by openssl static lib
            user32               # needed by openssl static lib
            crypt32              # needed by openssl static lib
        )

    else()
        message(FATAL_ERROR "Unsupported Compiler")
    endif()

elseif(APPLE)
    # uuid
    # set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -undefined dynamic_lookup")
    if(NOT DEFINED ENV{UUID_ROOT_DIR})
        message(SEND_ERROR "NOT DEFINED LIBUUID_PATH")
        message(SEND_ERROR "You can brew install util_linux or brew install e2fsprogs")
        message(SEND_ERROR "Then add UUID_ROOT_DIR=<util_linux/e2fsprogs> to environment")
        message(FATAL_ERROR "NOT DEFINED LIBUUID_PATH")
    endif()
    target_include_directories(${MODULE_NAME} PUBLIC $ENV{UUID_ROOT_DIR}/include)
    target_link_directories(${MODULE_NAME} PUBLIC $ENV{UUID_ROOT_DIR}/lib)

    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -mmacosx-version-min=10.13")
    # definitions
    target_compile_definitions(${MODULE_NAME}
        PUBLIC MAC
    )

    # lib-files
    target_link_libraries(${MODULE_NAME} PUBLIC
        uuid    
        pthread
        dl
    )

elseif(UNIX)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsigned-char")
    # definitions
    target_compile_definitions(${MODULE_NAME}
        PUBLIC LINUX
        PUBLIC GLIBCXX_USE_CXX11_ABI=0
    )

    # lib-files
    target_link_libraries(${MODULE_NAME} PUBLIC
        uuid
        rt
    )

else()
    message(FATAL_ERROR "Unsupported Platform")
endif()

string(REPLACE "/RTC1" "" CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")