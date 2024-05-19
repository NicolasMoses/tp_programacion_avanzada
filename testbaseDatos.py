
from fastapi import FastAPI
import csv
import psycopg2
from credential_DB import DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE, PORT

# Conexión a la base de datos
engine = psycopg2.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_DATABASE,
    port=PORT
)

Modelo = 'TopProduct'
ADV='IDOFCO721HTJGDH7332G'
cursor = engine.cursor()

if Modelo=="TopCTR":
    cursor.execute(f"""SELECT advertiser_id, product_id, dates,
    SUM(CASE WHEN type = 'click' THEN 1 ELSE 0 END) AS clicks,
    SUM(CASE WHEN type = 'impression' THEN 1 ELSE 0 END) AS impressions
    FROM ads_views
    GROUP BY advertiser_id, product_id, dates
    ORDER BY advertiser_id, product_id, dates;
                ;""")
    rows = cursor.fetchall()
    csv_filename = 'resultado.csv'
    # Abre el archivo CSV en modo de escritura
    with open(csv_filename, mode='w', newline='') as csv_file:
        # Crea un escritor CSV
        csv_writer = csv.writer(csv_file)

        # Escribe los encabezados de las columnas
        csv_writer.writerow(['advertiser_id','product_id','date','ctr'])

        # Escribe cada fila de datos en el archivo CSV
        for row in rows:
            ctr = row[3] / row[4] if row[4] != 0 else 0
            
            # Redondear el CTR a tres decimales
            ctr_rounded = round(ctr, 4)

            # Agregar el CTR redondeado a la fila antes de escribirla en el archivo CSV
            row_with_ctr = list(row)  # Convertir la tupla en lista para poder modificarla
            row_with_ctr.append(ctr_rounded)

            # Escribir la fila en el archivo CSV
            csv_writer.writerow(row_with_ctr[:3] + [row_with_ctr[5]])
    
elif Modelo=="TopProduct":
   # Consulta para recomendar los productos más visitados en la web del advertiser
        cursor.execute(f"""SELECT advertiser_id, product_id, date , COUNT(*) as visits FROM product_views 
        GROUP BY advertiser_id, product_id, date
        ORDER BY advertiser_id, product_id, date; """)
        #para exporta los datos

        rows = cursor.fetchall()
        # Nombre del archivo CSV donde se guardarán los datos
        csv_filename = 'resultado.csv'

        # Abre el archivo CSV en modo de escritura
        with open(csv_filename, mode='w', newline='') as csv_file:
            # Crea un escritor CSV
            csv_writer = csv.writer(csv_file)

            # Escribe los encabezados de las columnas
            csv_writer.writerow(['advertiser_id','product_id','date','count'])

            # Escribe cada fila de datos en el archivo CSV
            for row in rows:

                # Escribir la fila en el archivo CSV
                csv_writer.writerow(row)
  





