# README

Para ejecutar:

1. `pip install mpi4py`
2. `mpiexec -n <num> python tp3.py` donde `<num>` indica la cantidad de nodos MPI.

Tests:

El testbasico.txt joinea nodos, les asigna un file a cada uno, y luego verifica que se hayan asignado bien mediante el look-up.

El testdesordenado.txt es similar al test básico, pero tanto los stores como los look-up están desordenados para probar que el orden en el que se agregan y/o consultan los files no es relevante

El testintercalado.txt agrega los nodos, pero esta vez en el medio de los join hacemos un store en los nodos ya creados. Chequeamos que se hayan guardado bien y luego seguimos con más nodos repitiendo el proceso del store.

El teststore.txt prueba que funcione guardar varios files en un mismo nodo correctamente. Para esto se joinean varios nodos pero solo usamos uno para storear files. Luego chequeamos que en los otros nodos no exista ese file (en la consola el lookup nos va a mostrar que se devolvió "False") y comprobamos que estén todos en el nodo correspondiente.

Todos los test se pueden correr tanto para la versión bloqueante como para la no-bloqueante.


