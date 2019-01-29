#include<stdlib.h>
#include<stdio.h>
#include<string.h>
#include<math.h>
#include "mpi.h"

#define MAX_SIZE 128

void recursive(int A[],int q,int r){
int x,s,temp,i;
if (q < r){

     x = A[q];
     s = q;

    for (i = q + 1;i<r;i++){
     if (A[i] <=x ) {
        s = s + 1;
        temp = A[s];
        A[s] = A[i];
        A[i] = temp;
     }
    }
    temp = A[s];
    A[s] = A[q];
    A[q] = temp;

  recursive(A, q, s);
  recursive(A, s + 1, r);
}
}void PQsort(int nelements, int *elements, MPI_Comm comm)
{
int i,temp;
recursive(elements,0,(nelements));}

int main(int argc, char* argv[]){
int npes;
int myrank;
char fn1[255];
FILE* fp1;
int A[MAX_SIZE],i,size,noOfEle;
int *a,pivot,*smallArray,*largeArray;
int smallCount = 0;
int largeCount = 0;
int smallArraySize,largeArraySize;
int pProc;
int pSize=0,excess=0;
int *B,*E,pSizeInt,excessInt;
int x;
int color;
int *b,*e,bInt,eInt;
int sc = 0;
int lc = 0;
int *smallArrayUnion,*largeArrayUnion;
int pivotIndex;
int globalSmallCount=0,globalSmallCountSize;
int globalLargeCount=0,globalLargeCountSize;

MPI_Comm excessNetwork,oddNetwork,evenNetwork;
MPI_Group world_group;
MPI_Init(&argc, &argv);

/*get information about communicator*/
MPI_Comm_size(MPI_COMM_WORLD, &npes);
MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
MPI_Comm_group(MPI_COMM_WORLD, &world_group);

MPI_Group excessRanksGroup;
int oddCount=0,evenCount=0;
MPI_Group oddRanksGroup,evenRanksGroup;
int ec=0,oc=0;


//Fetching the data from the file and storing in an initial array
if(myrank==0){
        strcpy(fn1,"data.dat");
        fp1=fopen(fn1,"r");
        if(fp1==NULL){
            printf("cannot open the data file. \n\n");
            exit(1);
        }
       //Reading the values from the data file
        for(i=0; i<MAX_SIZE; i++){
             fscanf(fp1, "%d", &A[i]);
        }
       printf("Elements in the data file are:");
        //Printing the values in the data file
        for(i=0; i<MAX_SIZE; i++)
        {
                printf("%d", A[i]);
                printf(" ");
        }
}//End of execution for Process 0

if(npes != 1) {

//Checking if the array can be evenly distributed
if(MAX_SIZE%npes == 0){
        noOfEle = MAX_SIZE/npes;
        /*distribute data  from process 0 to all the other processes*/
        size=noOfEle*sizeof(int);
        a=(int *)malloc(size);
        MPI_Scatter(A, noOfEle, MPI_INT, a, noOfEle, MPI_INT, 0, MPI_COMM_WORLD);
        printf("The matrix elements distributed to process [%d]: \n", myrank);
        for(i=0; i<noOfEle; i++){
            printf("%d \n", a[i]);
        }
}else{
        pSize = (int)MAX_SIZE/npes;
       // printf("The size of pSize : %d",pSize);
        //pSize = 4;
        excess = MAX_SIZE%npes;
        x = MAX_SIZE - excess;
        pSizeInt = x*sizeof(int);
        excessInt = excess *sizeof(int);
        B = (int *)malloc(pSizeInt);
        E = (int *)malloc(excessInt);
        for(i=0;i<x;i++){
            B[i]=A[i];
        }
        for(i=0;i<excess;i++){
            E[i]=A[x+i];
        }
       /* if(myrank<excess){
            color = 1;
        }*/
        bInt = x*sizeof(int);
        eInt = excess *sizeof(int);
        b = (int *)malloc(bInt);
        e = (int *)malloc(eInt);
        MPI_Scatter(B, pSize, MPI_INT, b, pSize, MPI_INT, 0, MPI_COMM_WORLD);


        int excessRanks[excess];

        for(i=0;i<excess;i++){
            excessRanks[i] = i ;
        }
        MPI_Group_incl(world_group,excess,excessRanks,&excessRanksGroup);
       // MPI_Comm_split(MPI_COMM_WORLD, color, myrank, &excessNetwork);
        MPI_Comm_create(MPI_COMM_WORLD,excessRanksGroup,&excessNetwork);

       if(myrank < excess){
        MPI_Scatter(E, 1, MPI_INT, e, 1, MPI_INT, 0, excessNetwork);
        printf("The matrix elements distributed to process [%d]: \n", myrank);
        for(i=0; i<pSize; i++){
        printf("%d \n", b[i]);
        }
        printf("%d\n",e[0]);

        }
}
//Selecting processor randomly
if(myrank == 0){
     for(i=0;i<5;i++){
        pProc = rand() % npes;
     }
    //printf("random number is:%d\n",rand());
    printf("The randomly selected processor is %d: ",pProc);
    printf("\n");
}
//broadcasting the randomly selected processor from processor 0
MPI_Bcast(&pProc, 1, MPI_INT, 0, MPI_COMM_WORLD);
//printf("[%d]: After Bcast, random processor is %d\n", myrank, pProc);
//selecting pivot randomly
if(myrank == pProc){
        /*printf("Elements in random %d Processor: ", pProc);
           for(i=0;i<noOfEle;i++){
            printf("%d\n",a[i]);
            }*/
        for(i=0;i<3;i++){
            if(pSize==0){
             pivotIndex = rand() % noOfEle;
             pivot = a[pivotIndex];
            }else{
             pivotIndex = rand() % pSize;
             pivot = b[pivotIndex];
            }

    }

    printf("The Randomly selected pivot value from processor [%d] is %d\n",myrank,pivot);
}

//Broadcasting the pivot to all processors
MPI_Bcast(&pivot, 1, MPI_INT, pProc, MPI_COMM_WORLD);


if(pSize == 0){
for(i=0;i<noOfEle;i++){
    if(a[i]<=pivot){
        smallCount++;
    }else{
        largeCount++;
    }
}
}else{
    for(i=0;i<pSize;i++){
    if(b[i]<=pivot){
        smallCount++;
    }else{
        largeCount++;
    }
    }
    if(e[0]!=0){
    if(e[0]<=pivot){
        smallCount++;
    }else{
        largeCount++;
    }
    }

}
smallArraySize = smallCount*sizeof(int);
smallArray =(int *)malloc(smallArraySize);

largeArraySize = largeCount*sizeof(int);
largeArray =(int *)malloc(largeArraySize);

//printf("[%d] small array size is %d, large array size is %d\n",myrank,smallCount,largeCount);

//Dividing into small array and large array at each processor
if(pSize == 0){

            for(i=0;i<noOfEle;i++){
                if(a[i]<=pivot){
                    smallArray[sc] = a[i];
                    sc++;
                }
                else{
                    largeArray[lc] = a[i];
                    lc++;
                }
            }
}else{

            for(i=0;i<pSize;i++){
                if(b[i]<=pivot){
                    smallArray[sc] = b[i];
                    sc++;
                }else{
                largeArray[lc] = b[i];
                lc++;
             }
            }
            if(e[0]!=0){
            if(e[0]<=pivot){
                smallArray[sc] = e[0];
            }else{
                largeArray[lc] = e[0];
            }
            }
}
//printng the smallArray and largeArray
/*for(i=0;i<smallCount;i++){
printf("[%d] small Array elements are:%d\n",myrank,smallArray[i]);
}
for(i=0;i<largeCount;i++){
printf("[%d] large Array elements are:%d\n",myrank,largeArray[i]);
}*/
//Finding the total elements in A that are less than and greater than pivot.
if(myrank == 0){
    for(i=0;i<MAX_SIZE;i++){
        if(A[i]<=pivot){
            globalSmallCount++;
        }else{
            globalLargeCount++;
        }
    }
   // printf("[%d] globalsmallcount: %d\n",myrank,globalSmallCount);
   // printf("[%d] globallargecount: %d\n",myrank,globalLargeCount);

    globalSmallCountSize = (2 * MAX_SIZE) *sizeof(int);
    smallArrayUnion = (int *)malloc(globalSmallCountSize);
    globalLargeCountSize = (2 * MAX_SIZE)*sizeof(int);
    largeArrayUnion = (int *)malloc(globalLargeCountSize);

    for(i=0;i<(2*MAX_SIZE);i++){
    smallArrayUnion[i]= -1;
    }
}

//Broadcasting the globalsmallArraycount to all processors
//MPI_Bcast(&globalSmallCount, 1, MPI_INT, 0, MPI_COMM_WORLD);
//printf("[%d]: After Bcast, globalSmallCount is %d\n", myrank, globalSmallCount);

//Gathering the small Array and large Array elements from all processors
if(pSize == 0) {
MPI_Gather(smallArray,noOfEle,MPI_INT,smallArrayUnion,noOfEle,MPI_INT,0,MPI_COMM_WORLD);
MPI_Gather(largeArray,noOfEle,MPI_INT,largeArrayUnion,noOfEle,MPI_INT,0,MPI_COMM_WORLD);
}else{
MPI_Gather(smallArray,(pSize+1),MPI_INT,smallArrayUnion,(pSize+1),MPI_INT,0,MPI_COMM_WORLD);
MPI_Gather(largeArray,(pSize+1),MPI_INT,largeArrayUnion,(pSize+1),MPI_INT,0,MPI_COMM_WORLD);
}
int smallArrayUnionAtRootCount=0;
int largeArrayUnionAtRootCount=0;
int smallArrayUnionAtRoot[globalSmallCount];
int largeArrayUnionAtRoot[globalLargeCount];

if(myrank==0){

//printf("Global small array is:");
    //Printing the SmallArrayUnion
    for(i=0; i<(2*MAX_SIZE); i++){
        if(smallArrayUnion[i] > 0 && smallArrayUnion[i]<=MAX_SIZE){
            smallArrayUnionAtRoot[smallArrayUnionAtRootCount] = smallArrayUnion[i];
           // printf("%d\n",smallArrayUnionAtRoot[smallArrayUnionAtRootCount]);
            smallArrayUnionAtRootCount++;
        }
    }

//printf("Global large array is:");
    //Printing the largeArrayUnion
    for(i=0; i<(2*MAX_SIZE); i++){
        if(largeArrayUnion[i] > 0 && largeArrayUnion[i]<=MAX_SIZE){
            largeArrayUnionAtRoot[largeArrayUnionAtRootCount] = largeArrayUnion[i];
          //  printf("%d\n",largeArrayUnionAtRoot[largeArrayUnionAtRootCount]);
            largeArrayUnionAtRootCount++;
        }
      }
    }
for(i=0;i<npes;i++){
    if((i%2)==0){
        evenCount++;
    }else{
        oddCount++;
    }
}

int oddRanks[oddCount],evenRanks[evenCount];
for(i=0;i<npes;i++){
    if((i%2)==0){
        evenRanks[ec]=i;
      // printf("even %d\n",evenRanks[ec]);
        ec++;
    }else{
        oddRanks[oc]=i;
      // printf("odd %d\n",oddRanks[oc]);
        oc++;
    }
}

//printf("oddcount %d",oddCount);

//Creating a communication Network with Odd Number Processors
MPI_Group_incl(world_group,oddCount,oddRanks,&oddRanksGroup);
MPI_Comm_create(MPI_COMM_WORLD,oddRanksGroup,&oddNetwork);
//int s;
//MPI_Comm_size(oddNetwork, &s);

//printf("size of odd group is%d",s);
//Creating a communication Network with Even number processors
MPI_Group_incl(world_group,evenCount,evenRanks,&evenRanksGroup);
MPI_Comm_create(MPI_COMM_WORLD,evenRanksGroup,&evenNetwork);
int *scaterringArray;
int scaterringArraySize,totalScatteredArraySize;
scaterringArraySize = MAX_SIZE*sizeof(int);
scaterringArray = (int *)malloc(scaterringArraySize);

if(myrank == 0){

    int *smallArrayUnionAtR,*largeArrayUnionAtR;
    PQsort(smallArrayUnionAtRootCount,smallArrayUnionAtRoot,oddNetwork);
    PQsort(largeArrayUnionAtRootCount,largeArrayUnionAtRoot,evenNetwork);

  /*  printf("small array returned is:");
    for(i=0;i<smallArrayUnionAtRootCount;i++){
        printf("%d\n",smallArrayUnionAtRoot[i]);
    }
    printf("large array returned is:");
    for(i=0;i<largeArrayUnionAtRootCount;i++){
        printf("%d\n",largeArrayUnionAtRoot[i]);
    }*/


totalScatteredArraySize = (smallArrayUnionAtRootCount+largeArrayUnionAtRootCount);

//printf("scattered array size is:%d",totalScatteredArraySize);
int l=0;
    for(i=0;i<totalScatteredArraySize;i++){

        if(i<smallArrayUnionAtRootCount){
            scaterringArray[i] = smallArrayUnionAtRoot[i];
            //printf("ele is: ----%d",scaterringArray[i]);
        }
     else{
            scaterringArray[i] = largeArrayUnionAtRoot[l];
           l++;
        }

    }
  /*  printf("total scattered array is:");
    for(i=0;i<MAX_SIZE;i++){
        printf("%d\n",scaterringArray[i]);
    }*/

}
    MPI_Bcast(scaterringArray,MAX_SIZE, MPI_INT, 0, MPI_COMM_WORLD);
   // printf("[%d]broadcasted array is:",myrank);
  /* for(i=0;i<MAX_SIZE;i++){
        printf("---%d\n",scaterringArray[i]);
   }*/

   //Checking if the array can be evenly distributed
if(pSize == 0){
        /*distribute data  from process 0 to all the other processes*/
        int *s,sSize;
        sSize = noOfEle*sizeof(int);
        s = (int *)malloc(sSize);

        MPI_Scatter(scaterringArray,noOfEle, MPI_INT, s, noOfEle, MPI_INT, 0, MPI_COMM_WORLD);
        printf("The sorted matrix from processor [%d]: \n", myrank);
        for(i=0; i<noOfEle; i++){
            printf("%d \n", s[i]);
        }
}else{

        int excessR,xR,pSizeIntR,excessIntR,*BR,*ER,bIntR,eIntR,*bR,*eR;
        excessR = MAX_SIZE%npes;
        xR = MAX_SIZE - excessR;
        pSizeIntR = xR*sizeof(int);
        excessIntR = excessR *sizeof(int);
        BR = (int *)malloc(pSizeIntR);
        ER = (int *)malloc(excessIntR);
        for(i=0;i<xR;i++){
            BR[i]=scaterringArray[i];
        }
        for(i=0;i<excessR;i++){
            ER[i]=scaterringArray[xR+i];
        }

        bIntR = xR*sizeof(int);
        eIntR = excessR *sizeof(int);
        bR = (int *)malloc(bIntR);
        eR = (int *)malloc(eIntR);
        MPI_Scatter(BR, pSize, MPI_INT, bR, pSize, MPI_INT, 0, MPI_COMM_WORLD);

        int excessRanksR[excessR];

        for(i=0;i<excessR;i++){
            excessRanksR[i] = i ;
        }

       // MPI_Scatter(ER, 1, MPI_INT, eR, 1, MPI_INT, 0, excessNetwork);
        MPI_Send(ER,excessR,MPI_INT,(npes-1),123,MPI_COMM_WORLD);

        printf("The sorted matrix from processor[%d]: \n", myrank);
        for(i=0; i<pSize; i++) {
            printf("%d \n", bR[i]);
        }
        if(myrank == (npes-1))  {
            for(i=0;i<excessR;i++){
            printf("%d\n",ER[i]);
            }
        }
}


}else{

PQsort(MAX_SIZE,A,MPI_COMM_WORLD);
printf("\n The Sorted array using one processor is:");
for(i=0;i<MAX_SIZE;i++){
printf("%d\n",A[i]);
}

}


MPI_Finalize();
return 0;
}

