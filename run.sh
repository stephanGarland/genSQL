#!/usr/bin/env bash

read -ra SYSTEM_TYPE < <(uname -sm)
SYSTEM_TYPE=("${SYSTEM_TYPE[@],,}")

symlink() {
    local lib
    lib=$1
    cd library && ln -s "${SYSTEM_TYPE[0]}_${SYSTEM_TYPE[1]}_${lib}.so" "${lib}.so" && cd - || exit 1
}

case ${SYSTEM_TYPE[0]} in
"linux")
    case ${SYSTEM_TYPE[1]} in
    "x86_64")
        symlink "uuid"
        symlink "fast_shuffle"
        ;;
    *)
        printf "%s\n" "ERROR: No ${SYSTEM_TYPE[1]} libraries are available"
        printf "%s\n" "       Please compile them with make"
        ;;
    esac
    ;;
"darwin")
    case ${SYSTEM_TYPE[1]} in
    "x86_64" | "arm64")
        symlink "uuid"
        symlink "fast_shuffle"
        ;;
    *)
        printf "%s\n" "ERROR: No ${SYSTEM_TYPE[1]} libraries are available"
        printf "%s\n" "       Please compile them with make"
        ;;
    esac
    ;;
esac
