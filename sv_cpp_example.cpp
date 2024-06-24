#include <iostream>


int main(int argc, char *argv[]) {


  /**********   Initialize Input Graph    **********/


/* 
  // Example 1 - forest of stars
  int rows  = 15;
  int edges = 20;

  int row_offsets[rows+1] {0, 2, 7, 8, 9, 10, 11, 12, 12, 14, 15, 16, 17, 18, 19, 20};
  int col_indices[edges]  {3, 5, 4, 10, 12, 13, 14, 6, 0, 1, 0, 2, 9, 11, 8, 1, 8, 1, 1, 1};
*/

/* 
  // Example 2 - forest, same components
  int rows  = 15;
  int edges = 20;

  int row_offsets[rows+1] {0, 1, 3, 4, 6, 7, 8, 9, 9, 10, 12, 15, 16, 17, 19, 20};
  int col_indices[edges]  {3, 4, 10, 6, 0, 5, 1, 3, 2, 9, 8, 11, 1, 12, 13, 9, 10, 10, 14, 13};
*/


/*
  // Example 3 - complete graph on 6 vertices (K6)
  int rows  = 6;
  int edges = 30;

  int row_offsets[rows+1] {0, 5, 10, 15, 20, 25, 30};
  int col_indices[edges]  {1, 2, 3, 4, 5, 0, 2, 3, 4, 5, 0, 1, 3, 4, 5, 0, 1, 2, 4, 5, 0, 1, 2, 3, 5, 0, 1, 2, 3, 4};
*/


 
  // Example 4
  int rows  = 15;
  int edges = 34;

  int row_offsets[rows+1] {0, 2, 6, 9, 11, 14, 19, 21, 22, 24, 26, 28, 31, 33, 34, 34};
  int col_indices[edges]  {1, 3, 0, 2, 5, 10, 1, 4, 5, 0, 4, 2, 3, 5, 1, 2, 4, 7, 10, 8, 9, 5, 6, 11, 6, 11, 1, 5, 8, 9, 12, 11, 13, 12};



  /**********	Run Connected Components Algorithm	**********/

 
  /* Initialize the vector of connected component labels */

  int *D = (int *) calloc(rows, sizeof(int));


  /* Initially, each vertex is in its own putative connected component,
     so just label the component with the vertex label */

  for(int i = 0; i < rows; i++) {
    D[i] = i;
  }


  bool graft = true;

  int iterations = 0;


  /* Begin main Shiloach-Vishkin iteration */

  while(graft) {

    iterations++;

    graft = false;

    std::cout << " Shiloach-Vishkin iteration " << iterations << std::endl;

    /* Begin "Graft" phase - union operations */

    for(int i = 0; i < rows; i++) {

      for(int k = row_offsets[i]; k < row_offsets[i+1]; k++) {

        int row = i;
        int col = col_indices[k];
        int col_parent = D[col];
        int row_parent = D[row];

        /* Check whether or not i is pointing to a root and whether any of
           it's neighbors are pointing to a vertex with smaller label. */

        if( D[row] < D[col] && D[col] == D[D[col]] ) {
          D[D[col]] = D[row];
          graft = true;
        }

        if( D[col] < D[row] && D[row] == D[D[row]] ) {
          D[D[row]] = D[col];
          graft = true;
        }

      }  // End loop over vertex i's out edges

    }  // End "graft" phase


    /* Begin "Hook" phase - path compression */

    for(int i = 0; i < rows; i++) {

      /* While there exist paths of length 2 in the pointer graph, hook branches
         of separate rooted trees onto each other until it is a rooted star... */

      while( D[i] != D[D[i]] ) {
        D[i] = D[D[i]];
      }

    }  // End "hook" phase

  } // End Shiloach-Vishkin iteration


  /* Print out the connected component index for each vertex */

  std::cout << "Connected Component ID's: ";

  for(int i = 0; i < rows; i++) {
    std::cout << D[i] << " ";
  }

  std::cout << std::endl;

}
