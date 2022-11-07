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

    for i in range(head.columns.size):
        head.values[0][i]+=("_"+head.values[1][i])

    head.values[0] = [line.replace("_nan", "") for line in head.values[0]]
    head.values[0] = [line.replace(" ", "_") for line in head.values[0]]
    head.values[0] = [line.lower() for line in head.values[0]]
    df_columns = head.values[0]

    dataset = pd.read_fwf(
        filename,
        widths=[9, 16, 16, 77],
        header = None,
        skip_blank_lines=False,
    )
    dataset = dataset.drop([0,1,2,3])

    cluster = [str(line) for line in dataset[0]]
    cant_claves = [str(line) for line in dataset[1]]
    porc_claves = [str(line) for line in dataset[2]]
    princ_claves = [str(line) for line in dataset[3]]

    cluster_d = {}
    key = 1
    for line in cluster:
        if(line!="nan"):
            cluster_d[key] = cluster_d.get(key,"")+line
            key+=1

    cant_claves_d = {}
    key = 1
    for line in cant_claves:
        if(line!="nan"):
            cant_claves_d[key] = cant_claves_d.get(key,"")+line
            key+=1

    porc_claves_d = {}
    key = 1
    for line in porc_claves:
        if(line!="nan"):
            porc_claves_d[key] = porc_claves_d.get(key,"")+line
            key+=1

    princ_claves_d = {}
    key = 1
    for line in princ_claves:
        if(line!="nan"):
            princ_claves_d[key] = princ_claves_d.get(key,"")+line+" "
        else:
            key+=1

    df = cluster_d,cant_claves_d,porc_claves_d,princ_claves_d

    df = pd.DataFrame(df).transpose()
    df.columns = df_columns

    df['principales_palabras_clave'] = [line.replace("  ", " ") for line in df['principales_palabras_clave']]

    return df
