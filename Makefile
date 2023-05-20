CC := gcc
CFLAGS := -Wextra -Wall -O3
LDLIBS := -luuid
LDFLAGS := /usr/lib/x86_64-linux-gnu

ifeq ($(shell uname), Darwin)
	MAC_HEADER_DIR := $(shell find /opt/homebrew/Cellar/ossp-uuid -name "uuid.h" -exec dirname {} \;)
	MAC_HEADER_DIR := $(strip $(MAC_HEADER_DIR))
	LDFLAGS := /opt/homebrew/lib
endif

.PHONY: make
make:
	@if [ ! -f $(LDFLAGS)/libuuid.a ]; then \
		echo "FATAL: libuuid.a not found"; \
		echo "On MacOS, use: brew install ossp-uuid"; \
		echo "On Debian/Ubuntu, use: sudo apt-get install uuid-dev"; \
		echo "On CentOS/Fedora/RHEL, use: sudo yum install libuuid-devel"; \
		echo "On openSUSE, use: sudo zypper in libuuid-devel"; \
		exit 1; \
	fi
	$(CC) $(CFLAGS) -shared library/fast_shuffle.c -o library/fast_shuffle.so
	$(CC) $(CFLAGS) -shared library/uuid.c -L$(LDFLAGS) $(LDLIBS) -o library/uuid.so

.PHONY: clean
clean:
	rm -f library/fast_shuffle.so library/uuid.so
