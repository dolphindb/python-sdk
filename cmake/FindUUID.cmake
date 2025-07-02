# FindUUID.cmake

# === Find Header ===
find_path(UUID_INCLUDE_DIR 
    NAMES uuid.h
    PATH_SUFFIXES uuid
    HINTS
        $ENV{UUID_ROOT_DIR}/include
        $ENV{CONDA_PREFIX}/include
        ${CMAKE_PREFIX_PATH}/include
)

# === Find Shared ===
set(CMAKE_FIND_LIBRARY_SUFFIXES .so .dylib .lib)
find_library(UUID_SHARED_LIBRARY
    NAMES uuid
    HINTS
        $ENV{UUID_ROOT_DIR}/lib
        $ENV{CONDA_PREFIX}/lib
        ${CMAKE_PREFIX_PATH}/lib
)

# === Find Static ===
set(CMAKE_FIND_LIBRARY_SUFFIXES .a)
find_library(UUID_STATIC_LIBRARY
    NAMES uuid
    HINTS
        $ENV{UUID_ROOT_DIR}/lib
        $ENV{CONDA_PREFIX}/lib
        ${CMAKE_PREFIX_PATH}/lib
)

# === Restore defaults ===
unset(CMAKE_FIND_LIBRARY_SUFFIXES)

# === Find Shared ===
if(UUID_SHARED_LIBRARY)
    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(UUID DEFAULT_MSG UUID_SHARED_LIBRARY UUID_INCLUDE_DIR)

    # === Create shared target ===
    if(UUID_FOUND)
        add_library(UUID::UUID UNKNOWN IMPORTED)
        set_target_properties(UUID::UUID PROPERTIES
            IMPORTED_LOCATION "${UUID_SHARED_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${UUID_INCLUDE_DIR}"
        )
    endif()
endif()


# === Find Static ===
if(UUID_STATIC_LIBRARY)
    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(UUID DEFAULT_MSG UUID_STATIC_LIBRARY UUID_INCLUDE_DIR)

    # === Create static target ===
    if(UUID_FOUND)
        add_library(UUID::UUID_STATIC STATIC IMPORTED)
        set_target_properties(UUID::UUID_STATIC PROPERTIES
            IMPORTED_LOCATION "${UUID_STATIC_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${UUID_INCLUDE_DIR}"
        )
    endif()
endif()


# For backwards compatibility
set(UUID_INCLUDE_DIRS ${UUID_INCLUDE_DIR})
set(UUID_LIBRARIES ${UUID_SHARED_LIBRARY})
