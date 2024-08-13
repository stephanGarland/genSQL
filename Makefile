SHELL := /bin/bash

IS_MAC := False

BIN_DIR := gensql/lib/bin
SRC_DIR := gensql/lib/src

CC := gcc
CFLAGS := -Wextra -Wall -O3 -fPIC -shared
LDFLAGS := /usr/lib/x86_64-linux-gnu

ifeq ($(shell uname), Darwin)
	IS_MAC := True
	LDFLAGS := /opt/homebrew/lib
endif

LDSTATICLIBS := $(LDFLAGS)/libuuid.a

.PHONY: all check_glibc build clean

all:
ifeq ($(shell uname), Linux)
	@$(MAKE) check_glibc
endif
	@$(MAKE) build

_no_print := $(shell mkdir -p $(BIN_DIR))

check_glibc:
	@GLIBC_VERSION=$$(ldd --version | awk '{IGNORECASE=1} /glibc/ {print $$NF}'); \
	GLIBC_MAJOR=$$(echo $$GLIBC_VERSION | cut -d. -f1); \
	GLIBC_MINOR=$$(echo $$GLIBC_VERSION | cut -d. -f2); \
	if [[ $$GLIBC_MAJOR -gt 2 ]] || ([[ $$GLIBC_MAJOR -eq 2 ]] && [[ $$GLIBC_MINOR -ge 36 ]]); then \
		export NEED_LIBBSD=False; \
	else \
		export NEED_LIBBSD=True; \
	fi

build:
	@if [ ! -f $(LDFLAGS)/libuuid.a ]; then \
		echo "FATAL: libuuid.a not found"; \
		echo "On MacOS, use: brew install ossp-uuid"; \
		echo "On Debian/Ubuntu, use: sudo apt-get install uuid-dev"; \
		echo "On CentOS/Fedora/RHEL, use: sudo yum install libuuid-devel"; \
		echo "On openSUSE, use: sudo zypper install libuuid-devel"; \
		exit 1; \
	fi
	@if [ ! -f $(LDFLAGS)/libbsd.a ] && [[ $(IS_MAC) == "False" ]] && [ $$NEED_LIBBSD == "True" ]; then \
		echo "FATAL: libbsd.a not found"; \
		echo "On CentOS/Fedora/RHEL, use: sudo yum install libbsd-devel"; \
		echo "On Debian/Ubuntu, use: sudo apt-get install libbsd-dev"; \
		echo "On openSUSE, use: sudo zypper install libbsd-devel"; \
	fi
	$(CC) $(CFLAGS) $(SRC_DIR)/fast_shuffle.c -o $(BIN_DIR)/fast_shuffle.so
	$(CC) $(CFLAGS) $(SRC_DIR)/fast_div.c -o $(BIN_DIR)/fast_div.so
	$(CC) $(CFLAGS) $(SRC_DIR)/phone.c -o $(BIN_DIR)/phone.so
	$(CC) $(CFLAGS) $(SRC_DIR)/uuid.c -L$(LDFLAGS) $(LDLIBS) -o $(BIN_DIR)/uuid.so $(LDSTATICLIBS)
	$(CC) $(CFLAGS) $(SRC_DIR)/xoshiro.c -o $(BIN_DIR)/xoshiro.so

clean:
	rm -f $(BIN_DIR)/*.so
	rm -f $(BIN_DIR)/**/*.so
