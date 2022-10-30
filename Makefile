build:
	gcc tema1.c -o tema1 -Wall -lpthread -lm
build_debug:
	gcc tema1.c -o tema1 -Wall -lpthread -O0 -g3 -DDEBUG -lm
clean:
	rm tema1