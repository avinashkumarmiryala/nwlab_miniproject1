#any mistakes to be corrected later
CC = gcc
CFLAGS = -Wall -pthread

all: user1 user2

libsocket.a : ksocket.o
	ar rcs libsocket.a ksocket.o

ksocket.o : ksocket.c ksocket.h
	$(CC) $(CFLAGS) -c ksocket.c

user1: user1.c libsocket.a
	$(CC) $(CFLAGS) user1.c -L. -lksocket -o user1

user2: user2.c libsocket.a
	$(CC) $(CFLAGS) user2.c -L. -lksocket -o user2

clean:
	rm -f *.o *.a user1 user2