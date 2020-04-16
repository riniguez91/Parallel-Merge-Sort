from multiprocessing import Pool
import multiprocessing as mp
import random
import time

def merge(arrayLeft, arrayRight): # Funcion merge()
    mergedArray = []
    left_idx, right_idx = 0, 0
    while left_idx < len(arrayLeft) and right_idx < len(arrayRight):
        if arrayLeft[left_idx] <= arrayRight[right_idx]:
            mergedArray.append(arrayLeft[left_idx])
            left_idx += 1
        else:
            mergedArray.append(arrayRight[right_idx])
            right_idx += 1
        
    if left_idx < len(arrayLeft):
        mergedArray.extend(arrayLeft[left_idx:])
    if right_idx < len(arrayRight):
        mergedArray.extend(arrayRight[right_idx:])
    return mergedArray

def mergeSort(m): # Algoritmo mergeSort()
    if len(m) <= 1:
        return m
    
    middle = len(m) // 2
    left = m[:middle]
    right = m[middle:]

    left = mergeSort(left)
    right = mergeSort(right)

    return merge(left, right) 

def mergeWrap(left_and_right_arr): # Como le pasamos una tupla de arrays, asignamos cada uno a su variable correspondiente y hacemos el merge()
    left, right = left_and_right_arr
    return merge(left, right)

def mergeSortParallel(array_to_sort):
    n_cores = mp.cpu_count() # Nº de cores que se usaran para repartir el trabajo entre ellos
    size_chunks = len(array_to_sort) // n_cores # Nº de elementos que hay en cada chunk
    # Dividimos en chunks lo mas uniforme posible a la lista que a la vez se convierten en un iterable para la funcion Pool.map()
    chk_lst = [array_to_sort[i:i+size_chunks] for i in range(0, len(array_to_sort), size_chunks)]
    pool = Pool(processes=n_cores) # Establecemos el numero de procesos creados/utilizados como el numero de cores, por lo tanto paralelismo 
                                   # el cual implica concurrencia
    sorted_sublists = pool.map(mergeSort, chk_lst) # Hacemos merge sort de cada chunk de la lista

    # Al terminar, tenemos todos los chunks sorteados, pero nos falta hacer merge() con cada uno para finalizar el algoritmo
    while len(sorted_sublists) > 1: # Mientras haya mas de 1 chunk, significa que todavia no hemos terminado de merge todos los chunks
        # Hacemos un merge de cada chunk vecino, de tal forma que si tenemos un numero de chunks impar deajmos el ultimo chunk sin tocar para poder 
        # realizar el ultimo merge de ese chunk con el chunk de todos los merge anteriores, si el numero es par entonces realiza un 
        # merge de todos los chunks vecinos(de tupla en tupla) hasta que solo quede 1
        chk_lst = [(sorted_sublists[i], sorted_sublists[i+1]) for i in range(0, len(sorted_sublists)-1, 2)] 
        sorted_sublists = pool.map(mergeWrap, chk_lst) # Paralelizamos tambien la operacion mergeWrap(), mirar comentario de la funcion
    
    return sorted_sublists[0] # Todos los chunks se han combinado en una sola lista, por lo que en el primer y unico indice tenemos el resultado

if __name__ == "__main__":
    print("Generando array con 21,802,766 elementos...")
    inicio_garray = time.time()
    array_to_sort = [random.randint(0, 30_000) for i in range(21802766)] # Generamos 21_802_766 elementos que varian desde el 0 al 30_000 inclusive
    fin_garray = time.time()
    print("Array generado en: {:.2f}s".format(fin_garray-inicio_garray))
    inicio_pmerge = time.time()
    print("Ordenando array...")
    results = mergeSortParallel(array_to_sort)
    fin_pmerge = time.time()
    print("Operacion terminada!\nHa tardado en hacer merge sort parallel: {:.2f}s".format(fin_pmerge-inicio_pmerge))
    print(results[10_000:10_020]) # Imprimos 20 elementos para comprobar resultado

