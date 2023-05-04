# contar_palabras_quijote
Entrega de prpa sobre contar palabras del Quijote usando pyspark

En este repositorio encontramos un archivo .png que nos muestra cómo nos estamos conectándo al cluster para ejecutar los archivos python que se encuentran también en este repositorio.

El archivo selecionar_lineas.py resuelve el primer apartado de la práctica, en el cual se le mete como input el nombre del fichero del cual queremos seleccionar listas aleatoriamente, y el resultado (todas las listas elegidas) las escribe en un archivo de texto llamado "quijote_s05.txt". Para ello, creamos un rdd con las lineas del archivo 'input' y para cada linea en ese rdd, lo que se hace es lanzar un dado (valor aleatorio del 1 al 6) y obtener un porcentaje (valor aleatorio entre 0 y 100), y se compara si el valor del dado es menor que el porcentaje (esta condicion se cumplirá en la mayoría de casos). Si lo es, la linea se escribe en el fichero "quijote_s05.txt".
La manera de ejecutarlo es: python3 selecionar_lineas-py <infile>
  
El archivo contar_palabras.py resuelve el segundo apartado, el cual dado como input un archivo de texto, escribe en un fichero el número de palabras que tiene el archivo que se ha introducido.
Para ello, leemos el archivo y obtenemos un rdd de las lineas del archivo. Seguidamente, cada linea (aplicando map) borramos los caracteres especiales para quedarnos con cada palabra de cada línea. Seguidamente, aplicamos la funcion sum al rdd de palabras y ya tenemos el número de palabras del archivo.
Finalmente, si el archivo 'input' se llama "quijote_s05.txt", el resultado se escribe en "out_quijote_s05.txt", si es cualquier otro nombre, el número de palabras se escribe en "out_quijote.txt".
La manera de ejecutarlo es: python3 contar_palabras.py <infile>
  
Finalmente, el archivo contar_palabras_sleep.py es exactamente el mismo que el archivo contar_palabras.py, salvo que se le ha añadido un time.sleep(30) para tener mas tiempo para conectarse al cluster, pero funciona exactamente igual.
