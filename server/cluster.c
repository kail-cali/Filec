#include <stdio.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <string.h>
#include <arpa/inet.h>
#include <errno.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <pthread.h>
#define err(str) fprintf(stderr, str)

static volatile int SERVICE_KEEPALIVE;



/*----------class-----------*/
typedef struct Control{
    long start_time;
    int open_port; 
    int inet;
} Control;

typedef struct BinSemaphore{
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int v;
} BinSemaphore;

typedef struct Job{
    struct Job* prev;
    void (*function )(void* args);
    void* args;

} Job;

typedef struct JobQueue{
    pthread_mutex_t rmutex;
    Job* front;
    Job* rear;

    int len;
    BinSemaphore* has_job;

} JobQueue;


typedef struct Worker{
    pthread_t pthread;
    int id;

    struct Cluster* cluster;

    /*job*/
    int session;

} Worker;


typedef struct Cluster{
    pthread_t main_thread;
    pthread_mutex_t lock;
    pthread_cond_t idle;

    Worker** workers;
    int num_worker;
    int num_alive_worker;
    int num_working_worker;
    JobQueue job_queue;

    int server_fd;
    struct sockaddr_in serv_addr;
    int port;
    char inet[16];
    int connection;


} Cluster;


/*--------------------------*/

/*-----------call------------*/
struct Cluster;
struct Worker;

static int worker_init(Cluster* cluster, struct Worker** thread_p, int id);

/*--------------------------*/

static void bsem_init(BinSemaphore* bsem_p, int value){

    pthread_mutex_init(&(bsem_p->mutex), NULL);
    pthread_cond_init(&(bsem_p->cond),NULL);
    bsem_p->v = value;
}

static void bsem_reset(BinSemaphore* bsem_p){
    bsem_init(bsem_p, 0);
}

static void bsem_post(BinSemaphore* bsem_p){
    pthread_mutex_lock(&bsem_p->mutex);
    bsem_p->v = 1;
    pthread_cond_signal(&bsem_p->cond);
    pthread_mutex_unlock(&bsem_p->mutex);
}

static void wait(BinSemaphore* bsem_p){
    pthread_mutex_lock(&bsem_p->mutex);
    while (bsem_p->v != 1){
        pthread_cond_wait(&bsem_p->cond, &bsem_p->mutex);
    }
    bsem_p->v = 0;
    pthread_mutex_unlock(&bsem_p->mutex);

}


static void time_wait(BinSemaphore* bsem_p){
    // not used, aren't fit at job -shedule function
    pthread_mutex_lock(&bsem_p->mutex);
    while (bsem_p->v != 1){
        continue;
//        pthread_cond_timedwait(&bsem_p->cond, &bsem_p->mutex, 30);
    }
    printf("time out,");
}

/*--------------------------*/

static int job_queue_init(JobQueue* job_queue_p){
    job_queue_p->len =0;
    job_queue_p->front = NULL;
    job_queue_p->rear = NULL;
    
    job_queue_p->has_job = (struct BinSemaphore* )malloc(sizeof(struct BinSemaphore ));
    
    if (job_queue_p->has_job == NULL){
        err("job_queue_init():: could not allocate memory for binary semaphore in job queue");
        return -1;
    }
    pthread_mutex_init(&(job_queue_p->rmutex), NULL);
    bsem_init(job_queue_p->has_job, 0);
    

    return 0;
}

static void push(JobQueue* job_queue_p, struct Job* new_job){

    pthread_mutex_lock(&job_queue_p->rmutex);
    new_job->prev = NULL;

    if (job_queue_p->len==0){
        job_queue_p->front = new_job;
        job_queue_p->rear = new_job;
    }
    if (job_queue_p->len==1){
        new_job->prev = job_queue_p->front;
        job_queue_p->rear = job_queue_p ->front;
        job_queue_p-> front = new_job;

    }
    if (job_queue_p->len>=2){
        new_job->prev = job_queue_p->front;
        job_queue_p->front = new_job;
    }
    job_queue_p -> len ++ ;
    bsem_post(job_queue_p->has_job);
    pthread_mutex_unlock(&job_queue_p->rmutex);
    
}

static struct Job*  pop(JobQueue* job_queue_p){
    pthread_mutex_lock(&job_queue_p->rmutex);
    
    Job* front_job = job_queue_p->front;

    if (job_queue_p->len==0){
        pthread_mutex_unlock(&job_queue_p->rmutex);
        return NULL;
    }
    if (job_queue_p -> len==1){
        job_queue_p->front = NULL;
        job_queue_p->rear = NULL;
    }
    if (job_queue_p->len >=2){
        job_queue_p->front = front_job->prev;
    }
    
    
    job_queue_p->len --;
    pthread_mutex_unlock(&job_queue_p->rmutex);

    return front_job;
}


static void* worker_do(Worker* thread_p){
//wait 
    char worker_name[16] = {0};
    snprintf(worker_name, 16, "worker-%d", thread_p->id);
    Cluster* cluster_p = thread_p->cluster;
    
    
}


static void* cluster_do(Cluster* cluster){
    int new_session;
    printf("cluster do connection (%d)\n", cluster->connection);
    // open connection
    while (cluster->connection ){
        // call back?
        new_session = accept(cluster->server_fd, (struct sockaddr* )&(cluster->serv_addr), (socklen_t* )(sizeof(cluster->serv_addr )));
        if (new_session <0){
                err("Accept error found\n");
        }


        // push(new_session, new_job ) to jobqueue
        // new_session = -1;

    }
    
}





static int add_work(Cluster* cluster_p, void (*function_p)(void* ), void* args_p){
    Job* new_job;
    new_job = (struct Job* )malloc(sizeof(struct Job));
    if (new_job==NULL){
        err("add work():: could not allocate memory for new job \n");
        return -1;
    }
    new_job->function = function_p;
    new_job->args = args_p;

    push(&cluster_p->job_queue,new_job);

    return 0;
}



static int worker_init(Cluster* cluster, struct Worker** thread_p, int id){
    *thread_p = (struct Worker*)malloc(sizeof(struct Worker));
    if (thread_p==NULL){
        err("worker_init():: allocate worker on memory failed");
        return -1;
    }

    (*thread_p) -> cluster = cluster;
    (*thread_p) -> id = id;
    
    pthread_create(&(*thread_p)->pthread, NULL, (void * (*)(void* )) worker_do, (*thread_p));
    pthread_detach((*thread_p)->pthread);

    return 0;
}



static int _stream_init(Cluster** cluster){
    Cluster* cluster_p = (*cluster);
    cluster_p->port = 32209;
    int opt = 1;
    int server_fd;

    
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd <0){
        perror("stream_init() socket create failed \n");
        return -1;
    }
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))){
        perror("set socket opt\n");
        return -1;
    }
    cluster_p->serv_addr.sin_family=AF_INET;
    cluster_p->serv_addr.sin_addr.s_addr = INADDR_ANY;
    cluster_p->serv_addr.sin_port = htons(cluster_p->port);
    if (bind(server_fd, (struct sockaddr*)&(cluster_p->serv_addr), sizeof((cluster_p->serv_addr)))<0){
        err("stream_init : Bind Failed\n");
        return -1;
    }


    if (listen(server_fd, 3)<0){
        perror("listen Failed\n");
        return -1;
    }

    printf("stream done\n");
    cluster_p -> server_fd = server_fd;
    cluster_p->connection = 1;
    return 0;
}



struct Cluster* cluster_init(int num_worker){

    Cluster* cluster;
    cluster = (struct Cluster* )malloc(sizeof(struct Cluster ));
    if (cluster==NULL){
        err("cluster_init():: allocatate cluster on memory failed\n");
        return NULL;
    } 
    
    /*create job queue */
    if (job_queue_init(&cluster->job_queue) < 0){
        err("could not allocate job queue ");
        /*free here */
        free(cluster);
        return NULL;
    }


    cluster->workers = (struct Worker** )malloc(num_worker * sizeof(struct Worker* ));
    if (cluster->workers==NULL){

        err("cluster_init():: allocatate worker  on memory failed\n");
        return NULL;
    }
    
    /*create stream on cluster*/
    if (_stream_init(&cluster)<0){
        err("|_create stream Failed\n");
        return NULL;
    }
    /*create lock*/ 
    pthread_mutex_init(&(cluster->lock), NULL);
    pthread_cond_init(&cluster->idle, NULL);
    /*create worker thread */
    for (int i=0; i<num_worker; i++){
        worker_init(cluster, &cluster->workers[i], i );
        printf("Debug on cluster init : Work thread %d created\n", i);
    }

     
    return cluster;
}





int main(){
    
    SERVICE_KEEPALIVE =1;
    Cluster* cluster = cluster_init(10);
    /*stream processing start here */
    pthread_create(&(cluster->main_thread), NULL, (void * (*)(void* )) cluster_do, cluster);
    pthread_detach(cluster->main_thread);

    while ( SERVICE_KEEPALIVE){
        // wait
        continue;

        }
    return 0;
}
