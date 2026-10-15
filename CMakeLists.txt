cmake_minimum_required(VERSION 3.10)
project(SEAL_ORAM_NETIO CXX)

# ============================================================================
# Build settings
# ============================================================================
# C++17 is REQUIRED (was 14): ObjectKey.h / PartitionPosMap.h /
# Queries_Receiving use structured bindings, if-init statements,
# std::optional-style emplace results and inline constexpr.
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
	set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "" FORCE)
endif()

# Put executables in build/bin, libraries in build/lib
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
foreach(OUTPUTCONFIG IN ITEMS Debug Release RelWithDebInfo MinSizeRel)
	string(TOUPPER ${OUTPUTCONFIG} OUTPUTCONFIG_UPPER)
	set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_${OUTPUTCONFIG_UPPER} ${CMAKE_BINARY_DIR}/bin)
	set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_${OUTPUTCONFIG_UPPER} ${CMAKE_BINARY_DIR}/lib)
	set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_${OUTPUTCONFIG_UPPER} ${CMAKE_BINARY_DIR}/lib)
endforeach()

# Optional sanitizer build:  cmake -S . -B build -DSANITIZE=address,undefined
set(SANITIZE "" CACHE STRING "Comma-separated sanitizers (e.g. thread | address,undefined)")
if(SANITIZE)
	add_compile_options(-fsanitize=${SANITIZE} -fno-omit-frame-pointer -g)
	add_link_options(-fsanitize=${SANITIZE})
	message(STATUS "Sanitizers enabled: ${SANITIZE}")
endif()

# ============================================================================
# Dependencies
# ============================================================================
find_package(OpenSSL REQUIRED)
# std::thread is used by the proxy (Queries_Receiving) and by the parallel
# per-server dispatch in MultiRingORAM_Servers::executeOramOps.
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

find_library(EMP_LIBRARIES NAMES emp-tool)
find_library(CRYPTOPP_LIBRARY NAMES cryptopp)
find_path(CRYPTOPP_INCLUDE_DIR NAMES cryptopp/osrng.h)
if(NOT CRYPTOPP_LIBRARY)
	message(FATAL_ERROR "libcryptopp not found (brew install cryptopp / apt install libcrypto++-dev)")
endif()

# Homebrew paths only make sense on macOS; on Linux they just add noise.
if(APPLE)
	foreach(BREW_PREFIX /opt/homebrew /usr/local)
		if(EXISTS ${BREW_PREFIX}/include)
			include_directories(${BREW_PREFIX}/include)
			link_directories(${BREW_PREFIX}/lib)
		endif()
	endforeach()
endif()

include_directories(${CMAKE_SOURCE_DIR}/include ${CRYPTOPP_INCLUDE_DIR})

# ============================================================================
# Header lookup that tolerates a wrong-case file name.
# The code does  #include "ObjectKey.h" / "PartitionPosMap.h".  macOS is
# case-INsensitive so include/Objectkey.h works there, but Linux (EC2) is
# case-sensitive and the build breaks.  Resolve the real name here and warn.
# ============================================================================
function(depart_find_header out_var want)
	if(EXISTS ${CMAKE_SOURCE_DIR}/include/${want})
		set(${out_var} include/${want} PARENT_SCOPE)
		return()
	endif()
	file(GLOB candidates ${CMAKE_SOURCE_DIR}/include/*)
	foreach(c IN LISTS candidates)
		get_filename_component(base ${c} NAME)
		string(TOLOWER ${base} base_lc)
		string(TOLOWER ${want} want_lc)
		if(base_lc STREQUAL want_lc)
			message(WARNING
					"include/${base} should be named include/${want} — the sources "
					"#include \"${want}\".  This builds on macOS but FAILS on Linux.  Fix with:\n"
					"    git mv include/${base} include/${want}")
			set(${out_var} include/${base} PARENT_SCOPE)
			return()
		endif()
	endforeach()
	message(WARNING "include/${want} not found")
	set(${out_var} "" PARENT_SCOPE)
endfunction()

depart_find_header(HDR_OBJECTKEY  ObjectKey.h)
depart_find_header(HDR_POSMAP     PartitionPosMap.h)
depart_find_header(HDR_DRIVER     DePart_TPCC_Driver.h)
depart_find_header(HDR_RQ3        RQ3_Workload.h)
depart_find_header(HDR_RQ2        RQ2_Affinity.h)
depart_find_header(HDR_RQ4        RQ4_Scaling.h)

# ============================================================================
# Core library — everything the proxy + ORAM need
# ============================================================================
add_library(oram_core STATIC
		src/RingORAM.cpp
		src/RingORAM_helper.cpp
		src/NetIOConnector.cpp
		src/Util.cpp
		src/csv_reader.cpp
		src/TpccSchema.cpp
		src/generate_data.cpp
		src/Repartition.cpp
		src/MultiRingORAM_Servers.cpp
		src/Queries_Receiving.cpp
		# Header-only pieces listed so IDEs index them and so a header edit
		# triggers a rebuild:
		${HDR_OBJECTKEY}
		${HDR_POSMAP}
		include/MultiRingORAM_Servers.h
		include/Queries_Receiving.h
		include/Repartition.h
		include/RingORAM.h
		${HDR_RQ3}
		${HDR_RQ2}
		${HDR_RQ4}
		include/DataPath.h
		include/RQ_Preflight.h
)
# NOTE: nothing under Testing/ or TPC-C/ may be listed above — every one of
# those files has its own main(), and depart_add_exe_dir() / depart_add_exe()
# below already turn them into executables.  Putting them in the library gives
# "duplicate symbol _main" at link time.  The check right below enforces it.
get_target_property(_oram_core_srcs oram_core SOURCES)
foreach(_src IN LISTS _oram_core_srcs)
	if(_src MATCHES "^Testing/" OR _src MATCHES "^TPC-C/")
		message(FATAL_ERROR
				"oram_core must not contain '${_src}': files under Testing/ and TPC-C/ "
				"have their own main().  Remove that line from add_library(oram_core ...) "
				"— depart_add_exe_dir() already builds it as an executable.")
	endif()
endforeach()

target_include_directories(oram_core PUBLIC
		${CMAKE_SOURCE_DIR}/include
		${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(oram_core PUBLIC
		${CRYPTOPP_LIBRARY}
		OpenSSL::Crypto
		Threads::Threads
)
if(EMP_LIBRARIES)
	target_link_libraries(oram_core PUBLIC ${EMP_LIBRARIES})
endif()

# ============================================================================
# TPC-C support (data generation)
# ============================================================================
# NOTE (9/22/26): src/CsvWriter.cpp is NOT built.
#   include/CsvWriter.hpp is a HEADER-ONLY CsvWriter (its constructor and
#   operators have bodies in the header).  src/CsvWriter.cpp is a SECOND,
#   incompatible implementation of the same class — the upstream
#   tpcc-generator one, whose members are `out` / `firstWordInLine`, which the
#   header's class does not have.  Compiling both gives:
#       src/CsvWriter.cpp:10:12: error: redefinition of 'CsvWriter'
#       src/CsvWriter.cpp:14:9:  error: use of undeclared identifier 'out'
#   TpccGenerator.cpp compiles against the header alone, so the header is the
#   complete implementation and the .cpp is dead code.  Delete it when you get
#   the chance:  git rm src/CsvWriter.cpp
add_library(tpcc_support STATIC
		src/TpccGenerator.cpp
		include/TpccGenerator.hpp
		include/CsvWriter.hpp
)
target_link_libraries(tpcc_support PUBLIC oram_core)

# ============================================================================
# Helper: one line per executable
# ============================================================================
# Adds an executable, but SKIPS it (with a message) when the main source is
# missing — a renamed/removed test must not break the whole configure step.
function(depart_add_exe name)
	set(srcs ${ARGN})
	list(GET srcs 0 main_src)
	if(NOT EXISTS ${CMAKE_SOURCE_DIR}/${main_src})
		message(STATUS "skip target ${name}: ${main_src} not found")
		list(APPEND DEPART_SKIPPED "${name}")
		set(DEPART_SKIPPED "${DEPART_SKIPPED}" PARENT_SCOPE)
		return()
	endif()
	add_executable(${name} ${srcs})
	target_link_libraries(${name} PRIVATE oram_core)
	target_include_directories(${name} PRIVATE
			${CMAKE_SOURCE_DIR}/include
			${CMAKE_SOURCE_DIR}/src
	)
endfunction()

# One executable per .cpp found in a directory (target name = file stem).
# Used for the experiment folders so their exact file names don't matter.
function(depart_add_exe_dir dir)
	file(GLOB exp_srcs CONFIGURE_DEPENDS ${CMAKE_SOURCE_DIR}/${dir}/*.cpp)
	if(NOT exp_srcs)
		message(STATUS "skip ${dir}: no .cpp files")
		return()
	endif()
	set(made "")
	foreach(src IN LISTS exp_srcs)
		get_filename_component(stem ${src} NAME_WE)
		if(TARGET ${stem})
			message(STATUS "skip ${src}: target ${stem} already exists")
			continue()
		endif()
		add_executable(${stem} ${src})
		target_link_libraries(${stem} PRIVATE oram_core)
		target_include_directories(${stem} PRIVATE
				${CMAKE_SOURCE_DIR}/include
				${CMAKE_SOURCE_DIR}/src
		)
		list(APPEND made ${stem})
	endforeach()
	message(STATUS "${dir}: ${made}")
endfunction()

# ============================================================================
# Unit / integration tests
# ============================================================================
depart_add_exe(RingORAM_helper_test      "Testing/Unit Tests/RingORAM_main.cpp")
depart_add_exe(Repartition_UnitTests     "Testing/Unit Tests/Repartition_Test.cpp")
depart_add_exe(Server                    "Testing/Unit Tests/Server.cpp")
depart_add_exe(Client                    "Testing/Unit Tests/Client.cpp")
depart_add_exe(MultiRingORAM_Servers_main "Testing/Unit Tests/MultiRingORAM_Servers_main.cpp")
# Queries_Receiving_Test.cpp is an OLD copy of the MVTSO unit tests (pre-TxnEngine)
# and it needs the fake-ORAM stub header, not the real one, so it is not built
# here.  That suite now lives in tests/mvtso and runs via tests/run_tests.sh
# (or: cmake --build build --target logic_tests).
#depart_add_exe(Queries_Receiving_Test    "Testing/Unit Tests/Queries_Receiving_Test.cpp")
depart_add_exe(Servers_MultiRingORAM     "Testing/Unit Tests/Servers_MultiRingORAM.cpp")
depart_add_exe(MultiClients_QR_Test      "Testing/Unit Tests/MultiClients_QR_Test.cpp")
depart_add_exe(EndToEnd_Test             "Testing/Unit Tests/EndToEnd_Test.cpp")

# Uses the DePart driver (T1–T8 engine) on top of the ORAM cluster.
depart_add_exe(EndToEnd_FixedThreshold_Test
		"Testing/Unit Tests/EndToEnd_FixedThreshold_Test.cpp"
		${HDR_OBJECTKEY}
		${HDR_POSMAP}
		${HDR_DRIVER}
)
target_link_libraries(EndToEnd_FixedThreshold_Test PRIVATE tpcc_support)

# ============================================================================
# TPC-C benchmark (workload generator + DePart driver)
# ============================================================================
depart_add_exe(TPCC_ReadOnly
		"TPC-C/TPCC_Main.cpp")
target_link_libraries(TPCC_ReadOnly PRIVATE tpcc_support)
target_compile_definitions(TPCC_ReadOnly PRIVATE TPCC_READONLY_MIX)

# TPC-C data generator (the old standalone "tpcc-generator" binary):
#   ./build/bin/tpcc-generator <warehouses> <output_folder> [seed]
depart_add_exe(tpcc-generator "TPC-C/TPCC_Generator_Main.cpp")
if(TARGET tpcc-generator)
	target_link_libraries(tpcc-generator PRIVATE tpcc_support)
endif()

# Standard TPC-C (NewOrder/Payment/OrderStatus/Delivery/StockLevel):
#   SidePart (depart) vs Obladi (obladi) vs random+index (index)
#   ./build/bin/TPCC_Std run <data_dir> <k> <business_txns> <offered> <placement> ...
# Needs include/tpcc_std_workload.h and include/tpcc_proxy_index.h.
depart_add_exe(TPCC_Std "TPC-C/TPCC_Std_Main.cpp")
if(TARGET TPCC_Std)
	target_link_libraries(TPCC_Std PRIVATE tpcc_support)
endif()

# MVTSO correctness of the standard TPC-C txns + proxy indexes: the REAL epoch
# engine (src/Queries_Receiving.cpp) over an in-memory store (the test defines
# its own MultiRingORAM_Servers::executeOramOps, so it does NOT link oram_core).
#   cmake --build build --target tpcc_std_mvtso_test && ./build/bin/tpcc_std_mvtso_test
if(EXISTS ${CMAKE_SOURCE_DIR}/TPC-C/tpcc_std_mvtso_test.cpp)
	add_executable(tpcc_std_mvtso_test
			TPC-C/tpcc_std_mvtso_test.cpp
			src/Queries_Receiving.cpp)
	target_include_directories(tpcc_std_mvtso_test PRIVATE
			${CMAKE_SOURCE_DIR}/include ${CMAKE_SOURCE_DIR}/src ${CRYPTOPP_INCLUDE_DIR})
	target_link_libraries(tpcc_std_mvtso_test PRIVATE
			Threads::Threads ${CRYPTOPP_LIBRARY} OpenSSL::Crypto)
endif()

# ============================================================================
# Experiments — discovered from disk, so the exact file names don't matter.
#   Experiment1 = RQ2 privacy budget:     RQ2_PrivateBudget, Test_RQ2
#   Experiment2 = RQ3 distribution shift:  RQ3_DistShift,     Test_RQ3
#   Experiment3 = RQ4 shard scaling:       RQ4_ShardScaling,  Test_RQ4
# The old ORAM_Performance_* / Oram_Performance_*_Skewed microbenchmarks have
# been removed; nothing here refers to them by name, so their deletion needs no
# edit — the glob just stops finding them.  Each dir prints the targets it made.
# Target name = file stem, e.g. Testing/Experiment1/ORAM_Performance_100R.cpp
#   → target ORAM_Performance_100R
# After ADDING or RENAMING a file here, re-run cmake (CONFIGURE_DEPENDS makes
# most generators do it automatically).
# ============================================================================
depart_add_exe_dir("Testing/Experiment1")
depart_add_exe_dir("Testing/Experiment2")
depart_add_exe_dir("Testing/Experiment3")
depart_add_exe_dir("Testing/Insert-OnlyExperiment")

# The RQ experiments only need oram_core (already linked above); their data is
# generated in-process by RQ3_Workload.h, so they read no CSV and need no
# dataset.  Nothing extra to link.

# ============================================================================
# Logic-only test suites (no CryptoPP / NetIO needed)
#   cmake --build build --target logic_tests
# ============================================================================
if(EXISTS ${CMAKE_SOURCE_DIR}/tests/run_tests.sh)
	add_custom_target(logic_tests
			COMMAND bash ${CMAKE_SOURCE_DIR}/tests/run_tests.sh
			WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
			COMMENT "MVTSO / pos_map / e2e / flush / rq2 / rq3 / rq4 suites"
			USES_TERMINAL)
endif()
if(EXISTS ${CMAKE_SOURCE_DIR}/tests/preflight.sh)
	add_custom_target(preflight
			COMMAND bash ${CMAKE_SOURCE_DIR}/tests/preflight.sh ${CMAKE_SOURCE_DIR}/include
			WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
			COMMENT "Header / filename checks"
			USES_TERMINAL)
endif()

if(DEPART_SKIPPED)
	message(STATUS "Skipped targets (source missing): ${DEPART_SKIPPED}")
endif()
message(STATUS "Build type    : ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ standard  : ${CMAKE_CXX_STANDARD}")
message(STATUS "Crypto++      : ${CRYPTOPP_LIBRARY}")
message(STATUS "emp-tool      : ${EMP_LIBRARIES}")
message(STATUS "Binaries      : ${CMAKE_BINARY_DIR}/bin")