#include "ksocket.h"
#include <sys/shm.h>
#include <string.h>
#include <sys/select.h>


void* R(void *arg){
    //HARSHA

}
void* S(void *arg){
    //AVINASH

}

int main()
{
    pthread_t tidS, tidR;
    init();

    if(pthread_create(&tidS, NULL, S, NULL) != 0){
        perror("pthread_create S");
        exit(1);
    }

    if(pthread_create(&tidR, NULL, R, NULL) != 0){
        perror("pthread_create R");
        exit(1);
    }

    pthread_join(tidS, NULL);
    pthread_join(tidR, NULL);
    return 0;
}