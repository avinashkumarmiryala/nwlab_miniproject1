CC = gcc
CFLAGS = -Wall -pthread

ksocket.o : ksocket.c ksocket.h
	$(CC) $(CFLAGS) -c ksocket.c

libsocket.a : ksocket.o
	ar rcs libsocket.a ksocket.o

initksocket: initksocket.c ksocket.h libsocket.a
	$(CC) $(CFLAGS) initksocket.c -L. -lsocket -o initksocket

user1: user1.c libsocket.a
	$(CC) $(CFLAGS) user1.c -L. -lsocket -o user1

user2: user2.c libsocket.a
	$(CC) $(CFLAGS) user2.c -L. -lsocket -o user2

all: initksocket user1 user2

clean:
	rm -f *.o *.a user1 user2 initksocket