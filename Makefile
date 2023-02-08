make:
	gcc -Wextra -Wall -O3 -shared library/fast_shuffle.c -o library/fast_shuffle.so

clean:
	rm -f fast_shuffle.so
