CC := gcc
CFLAGS := -Wextra -Wall -O3 -fPIC -shared
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
	$(CC) $(CFLAGS) gensql/utils/libs/src/char_shuffle.c -o gensql/utils/libs/char_shuffle.so
	$(CC) $(CFLAGS) gensql/utils/libs/src/fast_shuffle.c -o gensql/utils/libs/fast_shuffle.so
	$(CC) $(CFLAGS) gensql/utils/libs/src/fast_mod.c -o gensql/utils/libs/fast_mod.so
	$(CC) $(CFLAGS) gensql/utils/libs/src/uuid.c -L$(LDFLAGS) $(LDLIBS) -o gensql/utils/libs/uuid.so

.PHONY: clean
clean:
	rm -f gensql/utils/libs/*.so 
