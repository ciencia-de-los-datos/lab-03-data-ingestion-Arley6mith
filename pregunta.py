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
    data = pd.read_fwf("clusters_report.txt", 
                     header=None, 
                     widths=[9, 16, 16, 77], 
                     names=
                     ["cluster",
                      "cantidad_de_palabras_clave",
                      "porcentaje_de_palabras_clave",
                      "principales_palabras_clave" ],
                     ).drop(range(0,3), axis=0).ffill()
    
    data.loc[:,"principales_palabras_clave"] = data["principales_palabras_clave"].str.replace(r'\s{2,}', ' ', regex=True)
    data.loc[:,"principales_palabras_clave"] = data["principales_palabras_clave"].str.replace('.', '')

    data = data.groupby(
        ["cluster",
         "cantidad_de_palabras_clave",
         "porcentaje_de_palabras_clave"], as_index=False).agg(
             {'principales_palabras_clave': lambda x: ' '.join(x)}).reset_index()
    
    data.rename(columns={None: 'principales_palabras_clave'},inplace=True)
    data.reset_index(drop=True, inplace=True)

    data.loc[:,"cluster"] = data["cluster"].astype(int)
    data.loc[:,"cantidad_de_palabras_clave"] = data["cantidad_de_palabras_clave"].astype(int)
    data.loc[:,"porcentaje_de_palabras_clave"] = data["porcentaje_de_palabras_clave"].str.rstrip("%").str.replace(',','.').astype(float)
    data.sort_values("cluster", inplace=True)
   
    return data

#print(ingest_data().principales_palabras_clave.to_list()[0])
