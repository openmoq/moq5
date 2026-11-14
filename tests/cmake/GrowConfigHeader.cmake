# Writes a copy of session.h in which two configs each have one more field
# after their last, so a private build of the core sees them grown. Nothing
# else in the copy differs from the header, except that its sibling includes
# are named through the include path: the copy does not sit beside them.
#
# Inputs: INPUT (core/include/moq/session.h), INCLUDE_DIR (core/include),
# OUTPUT (the copy to write).
cmake_minimum_required(VERSION 3.20)

foreach(var INPUT INCLUDE_DIR OUTPUT)
    if(NOT DEFINED ${var} OR "${${var}}" STREQUAL "")
        message(FATAL_ERROR "${var} is required")
    endif()
endforeach()

file(READ "${INPUT}" text)

# One appended field per config. Each closing line must occur exactly once: a
# header that renames, removes or repeats the record stops here.
foreach(type moq_done_subscribe_cfg_t moq_reject_subscribe_cfg_t)
    set(closing "} ${type};")
    string(LENGTH "${text}" with_length)
    string(REPLACE "${closing}" "" without "${text}")
    string(LENGTH "${without}" without_length)
    string(LENGTH "${closing}" closing_length)
    math(EXPR count "(${with_length} - ${without_length}) / ${closing_length}")
    if(NOT count EQUAL 1)
        message(FATAL_ERROR
            "'${closing}' occurs ${count} times in ${INPUT}; expected exactly one")
    endif()
    string(REPLACE "${closing}"
        "    uint64_t growth_probe_tail;\n${closing}" text "${text}")
endforeach()

# The sizes under test are the header's own. A header that does not define
# one of them is not the header this copy was written for.
foreach(macro MOQ_DONE_SUBSCRIBE_CFG_V0_SIZE
              MOQ_REJECT_SUBSCRIBE_CFG_V0_SIZE
              MOQ_REJECT_SUBSCRIBE_CFG_SIZE_THROUGH_REDIRECT)
    string(FIND "${text}" "#define ${macro} " at)
    if(at EQUAL -1)
        message(FATAL_ERROR "${INPUT} does not define ${macro}")
    endif()
endforeach()

# Sibling headers: "name.h" beside session.h becomes <moq/name.h>.
string(REGEX MATCHALL "#include \"[a-z0-9_]+\\.h\"" siblings "${text}")
if(siblings STREQUAL "")
    message(FATAL_ERROR "${INPUT} includes no sibling header; its form changed")
endif()
foreach(line IN LISTS siblings)
    string(REGEX REPLACE "#include \"([a-z0-9_]+\\.h)\"" "\\1" name "${line}")
    if(NOT EXISTS "${INCLUDE_DIR}/moq/${name}")
        message(FATAL_ERROR "${INPUT} includes \"${name}\", which is not in ${INCLUDE_DIR}/moq")
    endif()
endforeach()
string(REGEX REPLACE "#include \"([a-z0-9_]+\\.h)\"" "#include <moq/\\1>"
    text "${text}")

get_filename_component(output_dir "${OUTPUT}" DIRECTORY)
file(MAKE_DIRECTORY "${output_dir}")
file(WRITE "${OUTPUT}" "${text}")
