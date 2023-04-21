#include <unistd.h>
#include <stdint.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <errno.h>
#include <time.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#define err(str) fprintf(stderr, str)

/*-----class*/

static volatile int SERVICE_KEEPALIVE;

typedef struct BinSemaphore{
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int v;
} BinSemaphore;


typedef struct Job{
    struct Job* prev;
    void (*function)(void* args);
    void* args;

} Job;

typedef struct JobQueue{
    pthread_mutex_t rmutex;
    Job* front;
    Job* rear;
    BinSemaphore* has_job;
    int len;
} JobQueue;

typedef struct Context {
    int id;
    int fd;
    pthread_t pthread;
    int port;
    char inet[16];
    
    struct Driver* driver;
    struct sockaddr_in* client_addr;

} Context;

typedef struct Driver {
    Context** contexts;
    int num_context;
    int run_forever ;
    char history[1024];
    int check;
    JobQueue* job_queue;

} Driver;
/*--------------------------*/


/*-----call-----------------*/

static void* context_do(struct Context* thread_p);


static int _stream_init(Context*** thread_p);


static int bsem_init(BinSemaphore* bsem_p, int value);
static int job_queue_init(JobQueue* job_queue_p);

static volatile int SERVICE_KEEPALIVE;
/*-------------------------*/


static int job_queue_init(JobQueue* job_queue_p){
    
    job_queue_p -> front = NULL;
    job_queue_p -> rear = NULL;
    job_queue_p -> has_job = (struct BinSemaphore* )malloc(sizeof(struct BinSemaphore));
    job_queue_p -> len = 0;
    if (job_queue_p->has_job==NULL){
        err("job_queue_init():: binary setaphore allocation failed ");
        return -1;
    }
    pthread_mutex_init(&(job_queue_p->rmutex), NULL);
    bsem_init(job_queue_p->has_job, 0);
    return 0;
}

static int bsem_init(BinSemaphore* bsem_p, int value){
    

    pthread_mutex_init(&(bsem_p->mutex), NULL);
    pthread_cond_init(&(bsem_p->cond), NULL);
    bsem_p -> v = value;
        

    return 0;
}

static void bsem_switch(BinSemaphore* bsem_p){
    pthread_mutex_lock(&bsem_p->mutex);
    bsem_p->v = 1;
    pthread_cond_signal(&bsem_p->cond);
    pthread_mutex_unlock(&bsem_p->mutex);

}

static void wait(BinSemaphore* bsem_p){

    pthread_mutex_lock(&bsem_p->mutex);
    while (bsem_p->v !=1){
        pthread_cond_wait(&bsem_p->cond, &bsem_p->mutex);
    }
    bsem_p-> v = 0;
    pthread_mutex_unlock(&bsem_p->mutex);


}

static void push(JobQueue* job_queue_p, struct Job* new_job){
    /*Broadcast to work thread*/
    pthread_mutex_lock(&job_queue_p->rmutex);
    new_job->prev = NULL;

    if (job_queue_p->len==0){
        job_queue_p->front = new_job;
        job_queue_p->rear = new_job;
    }    
    if (job_queue_p->len==1){
        new_job->prev = job_queue_p->front;
        job_queue_p->rear = job_queue_p->front;
        job_queue_p->front = new_job;
    }
    if (job_queue_p->len>=2){
        new_job->prev = job_queue_p->front;
        job_queue_p->front = new_job;
    }

    
    job_queue_p->len ++;
    
   // bsme_on()
    pthread_mutex_unlock(&job_queue_p->rmutex);
}



static struct Job* pop(JobQueue* job_queue_p){
    pthread_mutex_lock(&job_queue_p->rmutex);

    Job* job_p = job_queue_p->front;

    if (job_queue_p->len==1){
        job_queue_p->front = NULL;
        job_queue_p->rear = NULL;

    }
    if (job_queue_p->len>=2){
        job_queue_p->front = job_p->prev;

    }
    
    if (job_queue_p->len>0){
        job_queue_p->len --;
    }
    pthread_mutex_unlock(&job_queue_p->rmutex);
    return job_p;
}


static int _stream_init(Context*** thread_p){
  //  printf("stream debug : %d\n", (**thread_p)->id);
    
    (**thread_p)->port =32209;
    char* tmp = "127.0.0.1";
    for (int i=0; i< strlen(tmp); i++){
        (**thread_p)->inet[i] = tmp[i]; 
    }
    int client_fd;

    (**thread_p)->client_addr = (struct sockaddr_in* )malloc(sizeof(struct sockaddr_in* ));
    if((**thread_p)->client_addr == NULL){
        printf("addr allocation failed");
        return -1;
    }
    if ((client_fd = socket(AF_INET, SOCK_STREAM, 0))< 0 ){
        printf("Socket creation error\n");
        return -1;

    }
    (**thread_p) -> fd = client_fd;
    (**thread_p)->client_addr->sin_family = AF_INET;
    (**thread_p)->client_addr->sin_port= htons((**thread_p)->port);
    if (inet_pton(AF_INET, (**thread_p)->inet, &(**thread_p)->client_addr->sin_addr)<=0){
        err("_stream_init(): bind failed, invalid address\n");
        return -1;
    } 
    return 0;
}
/*after context craeted, make stream in context, and give fd to context_thread*/



static void* context_do(struct Context* thread_p){
    /*fn for processing thread, same as excutor*/
    char debug_[17]= {0};
    snprintf(debug_, 17, "context-%d", thread_p->id);
    Driver* driver = thread_p->driver;
    int stream, valread;
    
    char buffer[1024]= {0};
    if (stream=connect(thread_p->fd, (struct sockaddr* )&thread_p->client_addr, sizeof(thread_p->client_addr))<0){
            err("Connect Error in context_do\n");
    }

    while (SERVICE_KEEPALIVE){
        // need wait until query recv
        // wait();
        wait(driver->job_queue->has_job);
        if (SERVICE_KEEPALIVE && stream >=0){
            //send(thread_p->client_fd, )
            continue;
        }

    }
}


void request_query(void* args){
    

}

int add_query(Driver* driver, void (*function_p)(void* ),  void*args_p){
    Job* new_job;
    new_job = (struct Job*)malloc(sizeof(struct Job));
    if (new_job==NULL){
        return -1;

    }
    new_job->function =function_p;
    new_job->args = args_p;
    Driver* driver_p = driver;

    push(driver_p->job_queue, new_job);

    return 0;
}

static int sample(Driver* driver){
    int i ; 
    for (i=0; i<10; i++){
        add_query(driver, request_query, (void*)(uintptr_t)i);
                    
    }

    return 0;
}


static int context_init(Driver* driver, struct Context** thread_p, int id){
    /*
    generated context structure,
    - allocated itself at memory
    - connect pointer to main thread(driver)
    - make stream for TCP-IP which used for submit query to Cluster(server side)

    parmas: driver  : structure for main thread, gave ptr
          : tread_p :
    return: {0:done well, -1:fail to allocated}

     * */
    *thread_p = (struct Context* )malloc(sizeof(struct Context));
    if (*thread_p == NULL){
        err("context_init(): could not allocate moemory for thread");
        return -1;
    }
    (*thread_p)->driver = driver;
    (*thread_p)->id = id;

    if (_stream_init(&thread_p) <0){
        err("|_stream_init(): error");
    }

    pthread_create(&(*thread_p)->pthread, NULL, (void * (*)(void *)) context_do, (*thread_p));
    pthread_detach((*thread_p)->pthread);
    /*
    pthread_create(&(*thread_p)->pthread, NULL, (void * (*)(void *)) thread_do, (*thread_p));
    pthread_detach((*thread_p)->pthread);
    */
    return 0;

}


struct Driver* driver_init(int num_context){
    Driver* driver;

    SERVICE_KEEPALIVE =1;
    driver  = (struct Driver* )malloc(sizeof(struct Driver));
    if (driver == NULL){
        err("driver_init(): Could not allocate memory for driver\n");
        return NULL;
    }
    driver->check = 1;

    /*make child thread(same as context) */
    driver->contexts = (struct Context** )malloc(num_context * sizeof(struct Context* ));
    if (driver->contexts==NULL){

        err("driver_init(): could not allocatie memory for context's thread");
        return NULL;
    }
    
    int n;
    for (n=0; n < num_context; n++ ){
        context_init(driver, &driver->contexts[n], n);

    }
    printf("Debug:: context id  %d\n", driver->contexts[0]->id );
    printf("Debug:: context port %d\n", driver->contexts[0]->port );
    printf("Debug:: context thread id%ld\n", driver->contexts[0]->pthread);
    printf("Debug:: context inet %s\n", driver->contexts[0]->inet);
    //char inet[dd15];
    return driver;
}

// currently single Context thread is forced



int main(){
    Driver* driver  = driver_init(1);
    driver -> run_forever = 0; 
    printf("all done:driver %d allocated at \n", driver->check);
    printf("runing \n");
    char buffer[1024];
    while (driver->run_forever==0){
        continue;
        /*
        printf("Querry input::\n ");
        fgets(buffer, sizeof(buffer), stdin);
        printf("file name is: %s", buffer);
        */
    }
    return 0;
}
