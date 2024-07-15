SHELL := /bin/bash

CC := gcc
CFLAGS := -Wextra -Wall -O3 -fPIC -shared
IS_MAC := False
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
	$(CC) $(CFLAGS) gensql/lib/src/fast_shuffle.c -o gensql/lib/bin/fast_shuffle.so
	$(CC) $(CFLAGS) gensql/lib/src/fast_mod.c -o gensql/lib/bin/fast_mod.so
	$(CC) $(CFLAGS) gensql/lib/src/uuid.c -L$(LDFLAGS) $(LDLIBS) -o gensql/lib/bin/uuid.so $(LDSTATICLIBS)
	$(CC) $(CFLAGS) gensql/lib/src/xoshiro.c -o gensql/lib/bin/xoshiro.so

clean:
	rm -f gensql/lib/bin/**/*.so
