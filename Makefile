make:
	gcc -Wextra -Wall -O3 -shared fast_shuffle.c -o fast_shuffle.so

clean:
	rm -f fast_shuffle.so
