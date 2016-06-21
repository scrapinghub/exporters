Merge two datasets removing duplicates
--------------------------------------

With this example, we are going to demonstrate how two different datasets can be merged removing 
duplicates in the process.

We are taking items from two different places (reader_1 and reader_2). In this case, they are two
random generators, but you can use any module provided by the project.

Then, we filter all the items not allowing duplicates in the "city" field. For more details about
the duplicates filter posibilities please take a look at the module docs.

Finally, we write the resulting items to the console.