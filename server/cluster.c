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
#include <dirent.h>
#define err(str) fprintf(stderr, str)



#define max(a,b) (((a)<(b))?(a):(b))
#define min(a,b) (((a)>(b))?(a):(b))



static volatile int SERVICE_KEEPALIVE;



/*----------class-----------*/
typedef struct Control{
    long start_time;
    int open_port; 
    int inet;
    int num_lock;
    int num_worker;
    int timeout;
    char root[20];
    
} Control;


typedef struct HashItem{
    char* key;
    char* value;

} HashItem;

typedef struct FileHash{
    HashItem** items;
    int size;
    int len;
    DIR* os_fd;
    struct dirent* connector;

    /*tmp file dir tree */
    char* file_path[30];
    int file_len;

} FileHash;



typedef struct File{
    char name[50];
    char contents[255];
    unsigned char contents_b[1024];
    int size;
    int find;
} File;

typedef struct Session{
    pthread_mutex_t timed_t;
    uintptr_t session_iid;
    int worker_id;
    struct timespec* timeout;
    char read_buffer[1024];
    int varlead_status;
    int time_out_default;
    
} Session;

typedef struct BinSemaphore{
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int v;
} BinSemaphore;

typedef struct Job{
    struct Job* prev;
    void (*function )(void* args);
    void* args;
    Session* info;
} Job;

typedef struct JobQueue{
    pthread_mutex_t rmutex;
    Job* front;
    Job* rear;

    int len;
    BinSemaphore* has_job;
    BinSemaphore** job_list;
    int lock_cnt;

} JobQueue;


typedef struct Worker{
    pthread_t pthread;
    int id;

    struct Cluster* cluster;
    int lock_idx;

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
    int* connection;

    FileHash* file_hash;

    Control control;


} Cluster;


/*--------------------------*/

/*-----------call------------*/
struct Cluster;
struct Worker;

static int worker_init(Cluster* cluster, struct Worker** thread_p, int id);

static int submit(Cluster* cluster_p, void(*function_p)(void* ), void* args_p );

void task_fn(void* args);


static void bsem_post(BinSemaphore* bsem_p);

void test_task_fn(void* args);

struct File* find_file(char* file_name);
static int submit_session(Cluster* cluster_p, void(*function_p)(void* ), void* session_p, char* buf_p);
/*--------------------------*/


static void bsem_list_init(BinSemaphore** bsem_list_p, int value){
    
    BinSemaphore* bsem_p = (*bsem_list_p);
   // pthread_mutex_init(&(bsem_p->mutex), NULL);
    /*

    pthread_mutex_init(&(bsem_p->mutex), NULL);
    pthread_cond_init(&(bsem_p->cond),NULL);
    bsem_p->v = value;
*/

}


static void bsem_list_post(BinSemaphore** bsem_list_p){

    BinSemaphore* bsem_p = (*bsem_list_p);
    
    pthread_mutex_lock(&bsem_p->mutex);
    bsem_p -> v = 1;
    pthread_cond_signal(&bsem_p->cond);
    pthread_mutex_unlock(&bsem_p->mutex);
}


static void bsem_list_wait(BinSemaphore** bsem_list_p){
    BinSemaphore* bsem_p = (*bsem_list_p);
    pthread_mutex_lock(&bsem_p->mutex);
    while (bsem_p->v != 1){
        pthread_cond_wait(&bsem_p->cond, &bsem_p->mutex);
    }
    bsem_p->v = 0;
    pthread_mutex_unlock(&bsem_p->mutex);
}



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


/*--------------------------*/
static int control_init(Control* control_p){
         



}


static int job_queue_init(JobQueue* job_queue_p){
    job_queue_p->len =0;
    job_queue_p->front = NULL;
    job_queue_p->rear = NULL;
    job_queue_p->lock_cnt = 3; 
    job_queue_p->has_job = (struct BinSemaphore* )malloc(sizeof(struct BinSemaphore ));
    
    if (job_queue_p->has_job == NULL){
        err("job_queue_init():: could not allocate memory for binary semaphore in job queue");
        return -1;
    }
    pthread_mutex_init(&(job_queue_p->rmutex), NULL);
    bsem_init(job_queue_p->has_job, 0);
    
    // hard coding;
    int num_lock;
    num_lock = 3;
    job_queue_p->job_list  = (struct BinSemaphore** )malloc(num_lock * sizeof(struct BinSemaphore *));
    if (job_queue_p->job_list==NULL){

        err("cluster_init():: allocatate job_list  on memory failed\n");
        return -1;
     }
    
    for (int i=0; i< 3; i++){
        bsem_list_init(&job_queue_p->job_list[i], 0);
        continue;
     }
     
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
        


    }
    else if (job_queue_p -> len==1){
        job_queue_p->front = NULL;
        job_queue_p->rear = NULL;
        job_queue_p -> len = 0;
    }
    else if (job_queue_p->len >=2){
        job_queue_p->front = front_job->prev;
        job_queue_p->len --;
        bsem_post(job_queue_p->has_job);

    }
    
    
    pthread_mutex_unlock(&job_queue_p->rmutex);

    return front_job;
}


static void* worker_do(Worker* thread_p){
    char worker_name[16] = {0};
    snprintf(worker_name, 16, "worker#%d", thread_p->id);
    Cluster* cluster_p = thread_p->cluster;
    
    pthread_mutex_lock(&cluster_p->lock);
    cluster_p->num_alive_worker +=1;
    pthread_mutex_unlock(&cluster_p->lock);

    while (SERVICE_KEEPALIVE) {
        wait(cluster_p->job_queue.has_job);
        if (SERVICE_KEEPALIVE){

            pthread_mutex_lock(&cluster_p->lock);
            cluster_p->num_working_worker +=1;
            pthread_mutex_unlock(&cluster_p->lock);



            void(* fn_buf)(void* );
            void* args_buf;
            Job* job_p = pop(&cluster_p->job_queue);
            if (job_p){
                job_p->info->worker_id = thread_p->id; 
                fn_buf = job_p->function;
                //args_buf = job_p->args;
                args_buf = job_p->info;
                
                fn_buf(args_buf);
                
                
                free(job_p->info);
                free(job_p);
            }
            /*temporerly give latnecy*/
            printf("sleep - latnecy::thraed[%s]  \n", worker_name);
            sleep(10);
            printf("wake up- latnecy::thraed[%s] \n", worker_name);
            /*delete later */
        }
        
        pthread_mutex_lock(&cluster_p->lock);
        cluster_p->num_working_worker -= 1;
        pthread_mutex_unlock(&cluster_p->lock);
    
    }

    pthread_mutex_lock(&cluster_p->lock);
    cluster_p->num_alive_worker -= 1;
    pthread_mutex_unlock(&cluster_p->lock);
    return NULL;
    
}

struct Session* create_session(){

    Session* session;
    session = (struct Session* )malloc(sizeof(struct Session));
    if (session ==NULL){
        err("could not allocated memory for sesseion\n");

        return NULL;
    }

    return session;
}


static void* test_cluster_do(Cluster* cluster){
    printf("\n==TEST MODE==\n");
    int session_len = sizeof(cluster->serv_addr);
    int new_session; 
    char read_buffer[1024];
    int varlead_status;
    while (SERVICE_KEEPALIVE && cluster->server_fd >0){
        /* create session */
        new_session = accept(cluster->server_fd, (struct sockaddr* )&(cluster->serv_addr), &session_len);
        if (new_session <0){
            err("cluster- Accept error found\n");
        }
       // varlead_status = read(new_session, read_buffer, 1024);    
       // printf("\t: Main Thread recv msg from Session[%d] ::  %s \n", new_session, read_buffer);
        /*create and submit session to job queue */

        submit(cluster, test_task_fn, (void* )(uintptr_t)new_session);
    //    submit_session(cluster, test_task_fn, (void* )(uintptr_t)new_session, &read_buffer);

        
   
    }
}

static void* cluster_do(Cluster* cluster){
    int new_session;
    int session_len = sizeof(cluster->serv_addr);

    printf("======Cluster working on main thread :: connection  status(%d)===\n",(cluster->server_fd>0));
    
    while (SERVICE_KEEPALIVE && cluster->server_fd >0){
        /* create session */
        new_session = accept(cluster->server_fd, (struct sockaddr* )&(cluster->serv_addr), &session_len);
        if (new_session <0){
            err("cluster- Accept error found\n");
        }
        
        /*create and submit session to job queue */
        submit(cluster, task_fn, (void* )(uintptr_t)new_session);
        printf("new session created {%d}\n", new_session);


    }
    
}

static int submit_session(Cluster* cluster_p, void(*function_p)(void* ), void* args_p, char* buf_p){
    printf("--test--debug submit seesion\n"); 
    Job* new_job;
 
    
    new_job = (struct Job* )malloc(sizeof(struct Job));
    if (new_job==NULL){
        err("at submit():: could not allocate memory for new job\n");
        return -1;
    }
         
    Session* session_p;
    /*process task at function */
     
    new_job->function = function_p;
//    new_job->args = args_p ;
    new_job->info = session_p;
    
    push(&cluster_p->job_queue, new_job);
    return 0;

}

static int submit(Cluster* cluster_p, void(*function_p)(void* ), void* args_p ){
    /*mapping session and schedule at job queue */
    Job* new_job;
 
    
    new_job = (struct Job* )malloc(sizeof(struct Job));
    if (new_job==NULL){
        err("at submit():: could not allocate memory for new job\n");
        return -1;
    }
         
    /*map session */
    Session* session_info;
    session_info = (struct Session* )malloc(sizeof(struct Session));
    if (session_info ==NULL){
    
        err("at submit():: could not allocate memory for new session\n");
        return -1;
    }
    session_info->session_iid = (uintptr_t) args_p;
    session_info->varlead_status;
    
    pthread_mutex_init(&(session_info->timed_t), NULL);

    session_info->timeout = (struct timespec* )malloc(sizeof(struct timespec ));
    if (session_info->timeout==NULL){
        err("at submit():: could not allocate memory for timeout");
        return -1;
    }
    /*--<#>---------default value hard coding------------------   */
    session_info->time_out_default = 90;


    /*process task at function */
     
    new_job->function = function_p;
    new_job->args = args_p ;
    new_job->info = session_info;
    
    push(&cluster_p->job_queue, new_job);
    return 0;

}


void test_task_fn(void* args){

    /*procceing session :: read find write send */
    int timeout = 0;
    
    Session* session_p = (Session* )args;
//    session_p->timeout->tv_sec = session_p->time_out_default; // default 90s
    File* file_p;
    printf("Thread #%d (%u)  Working on session[%ld] \n", session_p->worker_id , (int)pthread_self(), session_p->session_iid);

    while (timeout==0){

 
        session_p->varlead_status = read(session_p->session_iid, session_p->read_buffer, 1024);    
         printf("\t: T[%d] recv msg from Session[%ld] ::  %s \n",session_p->worker_id, session_p->session_iid,  session_p->read_buffer);
        
        file_p = find_file(session_p->read_buffer);        
        
        if (file_p==NULL){
            char no_file_msg[1024] = {0};
            snprintf(no_file_msg, 1024, "<csf>such a file name doesn't exists");
            send(session_p->session_iid, no_file_msg, strlen(no_file_msg),0);
            printf("\t:NO FILE::  T[%d] send msg to Session[%ld] \n",session_p->worker_id ,session_p->session_iid);
        }  
        else{

            send(session_p->session_iid, file_p->contents_b, sizeof(file_p->contents_b),0);
            printf("\t: T[%d] send msg to Session[%ld] \n",session_p->worker_id ,session_p->session_iid);
            free(file_p);
        }
        timeout = 1;
    }
}

void task_fn(void* args){
    /*procceing session :: read find write send */
    int timeout;
    
    Session* session_p = (Session* )args;
    session_p->timeout->tv_sec = session_p->time_out_default; // default 90s
    
    printf("Thread #%d (%u)  Working on session[%ld] \n", session_p->worker_id , (int)pthread_self(), session_p->session_iid);
    timeout = 0;
    while (timeout==0){

 
        session_p->varlead_status = read(session_p->session_iid, session_p->read_buffer, 1024);
    
        printf("\t: T[%d] recv msg from Session[%ld] ::  %s \n",session_p->worker_id, session_p->session_iid,  session_p->read_buffer);
    /*find file*/
        char find_file_loc[1024] = {0};
    
        snprintf(find_file_loc, 1024, "./tmp/loc/a.txt");
        send(session_p->session_iid, find_file_loc, strlen(find_file_loc),0);
        printf("\t: T[%d] send msg to Session[%ld] \n",session_p->worker_id ,session_p->session_iid);
        timeout = 1;
    }
    //close(session_p->session_iid);
}


static int worker_init(Cluster* cluster, struct Worker** thread_p, int id){
    *thread_p = (struct Worker*)malloc(sizeof(struct Worker));
    if (thread_p==NULL){
        err("worker_init():: allocate worker on memory failed");
        return -1;
    }
    
    (*thread_p) -> cluster = cluster;
    (*thread_p) -> id = id;
    (*thread_p) -> lock_idx =  (id % 3 ); 
    pthread_create(&(*thread_p)->pthread, NULL, (void * (*)(void* )) worker_do, (*thread_p));
    pthread_detach((*thread_p)->pthread);

    return 0;
}



static int _stream_init(Cluster** cluster){
    Cluster* cluster_p = (*cluster);
    cluster_p->port = 32209;
    int opt = 6;
    int server_fd;
//    server_fd = &(cluster_p->server_fd);
    
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    //cluster_p-> server_fd = socket(AF_INET, SOCK_STREAM, 0);
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

    cluster_p -> server_fd = server_fd;
    return 0;
}


struct File* find_file(char* file_name){
    File* file;
    char file_path[130] = {0};
    /*#hard coding */
    char* hard_path = "./server/book_file/";
    snprintf(file_path, 24,"%s", hard_path);
    file = (struct File*)malloc(sizeof(struct File));
    DIR* os_fd;
    struct dirent* connector;
    os_fd = opendir(hard_path);
    if (os_fd==NULL){
        err("dir does not exists\n");
        free(file);
        return NULL;
    }

    

    while ((connector = readdir(os_fd))!= NULL){
        char* d_name = connector->d_name;
        if (strlen(d_name) ==strlen(file_name)){
            int same=1;
            for (int i =0; i< strlen(d_name); i++){
                if (d_name[i]!= file_name[i]){
                    same = 0;
                    break;
                }
            }
            if (same){
                // write
                strcat(file_path, d_name);
                
                FILE* fptr;
                fptr = fopen(file_path, "r");
                
                if(fptr==NULL){
                    printf("\t at find file():: fptr error check file path %s \n", file_path);
                    break;
                }

                for (int i=0; i<10; i++){
                    fread((file->contents_b), 1, sizeof(file->contents_b), fptr);
                }
                snprintf(file->name,50, "%s", d_name);
                file->find = 1;
                fclose(fptr);
                return file;
            }
        }
    }

    
    
    free(file);
    return NULL;
}




static int  file_hash_init(Cluster* cluster, FileHash** file_hash){
    
    FileHash* file_hash_p = (*file_hash);
    file_hash_p = (struct FileHash* )malloc(sizeof(struct FileHash ));
    if (file_hash_p==NULL){
        err("|_ file_hash_init(): could not allocated file hash on memory");
        return -1;
    }
    
    /*hard coding dir*/
    file_hash_p -> os_fd = opendir("./server/book_file");
    
    if (file_hash_p->os_fd==NULL){
        err("dir does not exists\n");
        return -1;
    }

    
    int i = 0;

    while ((file_hash_p->connector = readdir(file_hash_p->os_fd))!= NULL){
        char* d_name = file_hash_p->connector->d_name;
        if (strlen(d_name)>3){
            file_hash_p->file_path[i] = (char *)malloc(sizeof(char));        
            file_hash_p->file_path[i] = d_name;
            i += 1;
        }
    } 
    
    
    file_hash_p->file_len = min(0, i-1);
    
    cluster->file_hash = file_hash_p;
    return 0;


}



struct Cluster* cluster_init(int num_worker){

    Cluster* cluster;
    cluster = (struct Cluster* )malloc(sizeof(struct Cluster ));
    if (cluster==NULL){
        err("cluster_init():: allocatate cluster on memory failed\n");
        return NULL;
    } 
     
    
    /*create job queue, terminate condition */
    if (job_queue_init(&cluster->job_queue) < 0){
        err("could not allocate job queue ");
        /*free here */
        free(cluster);
        return NULL;
    }

    /*not a terminate condition*/ 
    if (file_hash_init(cluster, &cluster->file_hash) < 0){
        err("could not allocate file hash");
    }

    //printf("--debug filehash tot len%d\n", cluster->file_hash->file_len);

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
        printf("cluster init : Work thread %d created\n", i);
    }

     
    return cluster;
}



void flush(Cluster* cluster_p){
    /*worker thread*/
    SERVICE_KEEPALIVE =0 ;
    for (int i=0; i < cluster_p->num_alive_worker; i++){

       free( cluster_p -> workers[i]);
    }


}

int main(){
    
    SERVICE_KEEPALIVE =1;
    Cluster* cluster = cluster_init(10);
    /*stream processing start here */
    //pthread_create(&(cluster->main_thread), NULL, (void * (*)(void* )) cluster_do, cluster);

    pthread_create(&(cluster->main_thread), NULL, (void * (*)(void* )) test_cluster_do, cluster);

    pthread_detach(cluster->main_thread);

    while ( SERVICE_KEEPALIVE){
        // wait
        continue;

        }
    SERVICE_KEEPALIVE = 0;
    printf("----Service Stop----");
    return 0;
}
