"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd


def ingest_data():

    #
    # Inserte su código aquí
    #
    #Ubicacion del archivo a leer
    filename = 'clusters_report.txt'
    #Se extrae el titulo o encabezado del archivo, teniendo en cuenta que es de dos renglones(filas)
    head = pd.read_fwf(
        filename,
        widths=[9, 16, 16, 76],
        header = None,
        nrows = 2,    
    )
    head.values[1] = [str(line) for line in head.values[1]]
    #Se ubica el nombre de las columnas en un solo renglon
    for i in range(head.columns.size):
        head.values[0][i]+=("_"+head.values[1][i])
    #Se organiza como se exige y se asigna a una lista
    head.values[0] = [line.replace("_nan", "") for line in head.values[0]]
    head.values[0] = [line.replace(" ", "_") for line in head.values[0]]
    head.values[0] = [line.lower() for line in head.values[0]]
    df_columns = head.values[0]
    #Se lee de nuevo el archivo
    dataset = pd.read_fwf(
        filename,
        widths=[9, 16, 16, 77],
        header = None,
        skip_blank_lines=False,
    )
    #Omitiendo las lineas donde esta alojado el nombre de las columnas
    dataset = dataset.drop([0,1,2,3])
    #Se asigna cada columna en un diccionario para hacer la respectiva organización
    cluster = [str(line) for line in dataset[0]]
    cant_claves = [str(line) for line in dataset[1]]
    porc_claves = [str(line) for line in dataset[2]]
    porc_claves = [line.replace(",",".") for line in porc_claves]
    princ_claves = [str(line) for line in dataset[3]]

    cluster_d = {}
    key = 1
    for line in cluster:
        if(line!="nan"):
            cluster_d[key] = cluster_d.get(key,0)+int(line)
            key+=1

    cant_claves_d = {}
    key = 1
    for line in cant_claves:
        if(line!="nan"):
            cant_claves_d[key] = cant_claves_d.get(key,0)+int(line)
            key+=1

    porc_claves_d = {}
    key = 1
    for line in porc_claves:
        if(line!="nan"):
            porc_claves_d[key] = porc_claves_d.get(key,0)+float(line[:-2])
            key+=1

    princ_claves_d = {}
    key = 1
    for line in princ_claves:
        if(line!="nan"):
            princ_claves_d[key] = princ_claves_d.get(key,"")+line+" "
        else:
            key+=1
    #Se unen los diccionarios de las columnas existente para tener un archivo compuesto tupla de diccionarios
    df = cluster_d,cant_claves_d,porc_claves_d,princ_claves_d
    #Se asigna el df a un DataFrame y se asignan los nombres de las columnas
    df = pd.DataFrame(df).transpose()
    df.columns = df_columns
    #Se realiza limpieza de las claves principales como se solicita
    df['principales_palabras_clave'] = [line.replace("    ", " ") for line in df['principales_palabras_clave']]
    df['principales_palabras_clave'] = [line.replace("   ", " ") for line in df['principales_palabras_clave']]
    df['principales_palabras_clave'] = [line.replace("  ", " ") for line in df['principales_palabras_clave']]
    df['principales_palabras_clave'] = [line.replace(".", "") for line in df['principales_palabras_clave']]
    df['principales_palabras_clave'] = [line.replace(".", "") for line in df['principales_palabras_clave']]
    df['principales_palabras_clave'] = [line[:-1] for line in df['principales_palabras_clave']]

    return df
