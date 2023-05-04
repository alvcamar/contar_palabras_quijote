#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: alvarocamarafernandez
"""

from pyspark import SparkContext
import sys
import string
import time

def escribeFichero(filename, txt):
    with open(filename, "w") as f:
        f.write(txt)
    print("Información guardada en el fichero " + filename + " correctamente.")

def word_split(line):
    for c in string.punctuation+"¿!«»":
        line = line.replace(c,' ')
        line = line.lower()
    return len(line.split())

def main(filename):
    with SparkContext() as sc:
        sc.setLogLevel("ERROR")
        lineas = sc.textFile(filename)
        words_rdd = lineas.map(word_split)
        print (words_rdd.collect())
        suma = words_rdd.sum()
        print ("El valor de la suma es: " + str(suma))
        if filename == "quijote_s05.txt":
            escribeFichero("out_quijote_s05.txt", str(suma))
        else:
            escribeFichero("out_quijote.txt", str(suma))

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python3 {0} <file>".format(sys.argv[0]))
    else:
        main(sys.argv[1])
        print("El programa ya ha terminado. Le ponemos un 'sleep' para simular que tarda un poco mas y poder meternos al puerto sin fallos.")
        time.sleep(30)
