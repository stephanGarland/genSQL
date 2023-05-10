make:
	gcc -Wextra -Wall -O3 -shared library/fast_shuffle.c -o library/fast_shuffle.so

	gcc -Wextra -Wall -O3 -shared library/uuid.c -luuid -o library/uuid.so
clean:
	rm -f library/fast_shuffle.so
	rm -f library/uuid.so
