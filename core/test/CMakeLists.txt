cmake_minimum_required(VERSION 3.4.1)
set(ProjectName gt_main)
project(${ProjectName})

set(TEST_SRC ${CMAKE_CURRENT_SOURCE_DIR})

set(CMAKE_CXX_STANDARD 11)

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0")
set(CMAKE_EXE_LINKER_FLAGS,"-Wl,-rpath,.")

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BUILD_DIR})

#===============================================================
# Find GoogleTest
set(GOOGLETEST_VERSION 1.12.1)
add_subdirectory(${TEST_SRC}/thirdparty/googletest)
include_directories(${TEST_SRC}/thirdparty/googletest/include)

#===============================================================
# Find OpenSSL
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
include_directories(${OPENSSL_INCLUDE_DIR})

#===============================================================
# Find Python3
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

include_directories(${PYTHON_INCLUDE_DIR})
link_directories(${PYTHON_LIBDIR})

message(STATUS "[Python Found: ${Python_FOUND}]")
message(STATUS "[Python Include PATH: ${Python_INCLUDE_DIRS}]")
message(STATUS "[Python Executable PATH: ${Python_EXECUTABLE}]")
message(STATUS "[Python Lib PATH: ${Python_LIBRARY}]")


#===============================================================
# Find Pybind11
execute_process(COMMAND pybind11-config --cmakedir OUTPUT_VARIABLE pybind11_DIR)
file(TO_CMAKE_PATH ${pybind11_DIR} pybind11_DIR)
string(REGEX REPLACE "\n$" "" pybind11_DIR ${pybind11_DIR})
find_package(pybind11 CONFIG REQUIRED)
include_directories(${pybind11_INCLUDE_DIRS})
message(STATUS "[pybind11 ${pybind11_VERSION}]")
message(STATUS "[pybind11_INCLUDE_DIRS ${pybind11_INCLUDE_DIRS}]")


#===============================================================
# Add src
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../include)
aux_source_directory(../src SRC)
list(REMOVE_ITEM SRC "../src/binding.cpp")

#===============================================================
# Add thirdpartys to src
aux_source_directory(../thirdparty/abseil/src ABSL_SRC)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../thirdparty/abseil/include)
list(APPEND SRC ${ABSL_SRC})
message(${SRC})

#===============================================================
# Add test to src
file(GLOB TEST_SRC "src/*_gtest.cpp")

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

add_executable(${ProjectName}
    ${SRC}
    ${TEST_SRC}
    "src/config.cpp"
    "src/gt_main.cpp"
)

message(STATUS "[OPENSSL_LIBRARIES ${OPENSSL_LIBRARIES}]")
message(STATUS "[PYTHON_LIBDIR ${PYTHON_LIBDIR}]")


target_compile_definitions(${ProjectName}
    PUBLIC LINUX
    PUBLIC GLIBCXX_USE_CXX11_ABI=0
)

target_link_libraries(${ProjectName}
    pthread
    uuid
    rt
    gtest_main
    ${PYTHON_LIBRARY}
    ${OPENSSL_LIBRARIES}
)
