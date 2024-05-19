from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
import uvicorn
from datetime import datetime, timedelta
from collections import defaultdict
from credential_DB import DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE, PORT
import os


# Conexión a la base de datos
engine = psycopg2.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_DATABASE,
    port=PORT
 )


#Instancia FastAPI
app = FastAPI()


######---------------------END POINT de bienvenida-------------------######
@app.get("/")
def root():
    return {
            'mensaje': 'Hola esto es un modelo de recomendación',
            'endpoints': [
                {
                    'endpoint': '/recommendations/(advertiser_id)/(top_product_df" o "top_ctr_df)',
                    'definición': 'Devuelve las recomendaciones del día',
                    'ejemplo': '/recommendations/IDOFCO721HTJGDH7332G/top_product_df'
                },
                {
                    'endpoint': '/stats/',
                    'definición': 'Devuelve 3 métricas de recomendación por advertiser últimos 7 días, estabilidad en la recomendación por modelo en los últimos 7 días, advertiser_id diferentes',
                    'ejemplo': '/stats/'
                },
                {
                    'endpoint': '/history/(advertiser_id)/(top_product_df" o "top_ctr_df)',
                    'definición': 'Histórico de advertiser en función del modelo indicado últimos 7 días',
                    'ejemplo': '/history/IDOFCO721HTJGDH7332G/top_product_df'
                }
            ]
        }

######---------------------Validadores de entrada---------------------######
modelos_validos = ["top_product_df", "top_ctr_df"]

def check_advertiser_exists(advertiser_id):
    # Crear cursor para ejecutar consultas SQL
    cursor = engine.cursor()
    # Consulta SQL para verificar si el advertiser_id existe en la tabla advertiser
    cursor.execute(f"SELECT * FROM advertiser_ids WHERE advertiser_id = '{advertiser_id}' LIMIT 1;")
    exists = cursor.fetchone() is not None
    # Cerrar cursor
   # cursor.close()
    return exists


######-------END POINT   devuelve las recomendaciones del día---------######

@app.get("/recommendations/{adv}/{modelo}")
def adv_recomendation_model(adv: str, modelo: str = None):
    
    # Error para modelos no validos
    if modelo not in modelos_validos:
        raise HTTPException(status_code = 400,
                            detail = "El modelo especificado no es valido, seleccione entre top_product_df  o  top_ctr_df")

    # Error para adv no validos 
    if not check_advertiser_exists(adv):
        raise HTTPException(status_code=400, detail="El advertiser_id especificado no es válido, verifique el ingresado")
  
    
    # Consulta segun el tipo de modelo especificado
    cursor = engine.cursor()

    try:
    
        if modelo == "top_product_df":
    
            cursor.execute(f"""SELECT product_id, count
                             FROM top_product_df 
                             WHERE advertiser_id = '{adv}' AND date = CURRENT_DATE - INTERVAL '1 days'   
                             ORDER BY count DESC;"""
                        )
        else:
            cursor.execute(f"""SELECT product_id, ctr
                             FROM top_ctr_df 
                             WHERE advertiser_id = '{adv}' AND date = CURRENT_DATE - INTERVAL '1 days'  
                             ORDER BY CTR DESC;"""
                        )

           
    
        # Obtener los resultados de la consulta
        rows = cursor.fetchall()

        # Convertir los resultados en formato JSON
        results = []
        cant_model_name= 'count' if modelo=="top_product_df" else 'ctr'
        for row in rows:
            results.append({ 
                'product_id': row[0],
                'model':modelo,
                f'{cant_model_name}':row[1]
            }) 

        # Cerrar cursor y conexión

    # Retornar los resultados en formato JSON
        return results
    
    finally:
        cursor.close()


######-------------END POINT   devuelve diferentes metricas--------------######
@app.get("/stats/")
def get_stats():
    cursor = engine.cursor()

    try:
        # Metrica 1: Listado de todos los advertiser_id diferentes
        cursor.execute("SELECT DISTINCT advertiser_id FROM top_product_df;")
        rows_advertiser_product = cursor.fetchall()

        cursor.execute("SELECT DISTINCT advertiser_id FROM top_ctr_df;")
        rows_advertiser_ctr = cursor.fetchall()

        all_advertiser_ids = set()
        for row in rows_advertiser_product:
            all_advertiser_ids.add(row[0])
        for row in rows_advertiser_ctr:
            all_advertiser_ids.add(row[0])
        all_advertiser_ids = list(all_advertiser_ids)

        resultsm1 = [{'advertiser_id': advertiser_id} for advertiser_id in all_advertiser_ids]

        # Metrica 2: Variacion en la recomendacion por advertiser ultimos 7 días
        cursor.execute(f"""SELECT advertiser_id, SUM(conteo_diferentes) AS diferentes
                            FROM(
	                            SELECT advertiser_id, date, COUNT(product_id) AS conteo_diferentes
	                            FROM top_product_df
	                            WHERE date >= CURRENT_DATE - INTERVAL '7 days'
	                            GROUP BY advertiser_id, date
	                            ORDER BY advertiser_id DESC
	                            )
                            GROUP BY advertiser_id
                            ORDER BY diferentes DESC
                            LIMIT 20;""")
        rows_top_product = cursor.fetchall()

        cursor.execute(f"""SELECT advertiser_id, SUM(conteo_diferentes) AS diferentes
                            FROM(
	                            SELECT advertiser_id, date, COUNT(product_id) AS conteo_diferentes
	                            FROM top_ctr_df
	                            WHERE date >= CURRENT_DATE - INTERVAL '7 days'
	                            GROUP BY advertiser_id, date
	                            ORDER BY advertiser_id DESC
	                            )
                            GROUP BY advertiser_id
                            ORDER BY diferentes DESC
                            LIMIT 20;""")
        rows_top_ctr = cursor.fetchall()

        resultsm2 = []  
        for row in rows_top_product:
            resultsm2.append({ 
                'advertiser_id_top_product': row[0],
                'scoring_iguales_top_product': row[1]
            }) 

        for row in rows_top_ctr:
            resultsm2.append({
                'advertiser_id_top_ctr': row[0],
                'scoring_iguales_top_ctr': row[1]
            })

        # Metrica 3: Advertisers con mas estabilidad en la recomendacion por modelo en los ultimos 7 días
        scores = defaultdict(int)

        cursor.execute("SELECT advertiser_id, product_id, date FROM top_product_df;")
        rows_product = cursor.fetchall()

        cursor.execute("SELECT advertiser_id, product_id, date FROM top_ctr_df;")
        rows_ctr = cursor.fetchall()

        # Construir un diccionario para rastrear los productos recomendados por ambos modelos
        products_both_models = defaultdict(set)
        for adv_id, product_id, date in rows_product:
            products_both_models[(adv_id, date)].add(product_id)
        for adv_id, product_id, date in rows_ctr:
            products_both_models[(adv_id, date)].add(product_id)

        # Calcular el score para cada anunciante
        for (adv_id, date), products in products_both_models.items():
            scores[adv_id] += len(products)

        # Ordenar los scores y seleccionar los 20 primeros
        scores_sorted = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:20]

        resultsm3 = [{"advertiser_id_mas_estables": adv_id, "score": score} for adv_id, score in scores_sorted]

        return {"Métrica1:Listado de todos los advertiser_id diferentes": resultsm1, "Métrica2:Variación en la recomendación por advertiser últimos 7 días": resultsm2, "Metrica3:Advertisers con mas estabilidad en la recomendación por modelo en los últimos 7 días ": resultsm3}

    finally:
        cursor.close()
        
######-------------END POINT   de historico de advertiser en funcion del modelo indicado--------------######

@app.get("/history/{adv}/{modelo}")
def get_history(adv: str, modelo: str):
    
    # Error para modelos no validos
    if modelo not in modelos_validos:
        raise HTTPException(status_code = 400,
                            detail = "El modelo especificado no es valido, seleccione entre top_product_df  o  top_ctr_df"
    )

        # Error para adv no validos 
    if not check_advertiser_exists(adv):
        raise HTTPException(status_code=400, detail="El advertiser_id especificado no es válido, verifique el ingresado")

    cursor = engine.cursor()

    try:
        if modelo == "top_product_df":
            # Consulta para recomendar los productos más visitados en la web del advertiser
            cursor.execute(f"""SELECT product_id, count 
                             FROM top_product_df 
                             WHERE advertiser_id = '{adv}' AND date >= CURRENT_DATE - INTERVAL '7 days'
                             ORDER BY date DESC, count DESC;"""
            )

        else:
            cursor.execute(f"""SELECT product_id, ctr 
                             FROM top_ctr_df 
                             WHERE advertiser_id = '{adv}' AND date >= CURRENT_DATE - INTERVAL '7 days'
                             ORDER BY date DESC, ctr DESC;"""
        )

        # Obtener los resultados de la consulta
        rows = cursor.fetchall()

        # Convertir los resultados en formato JSON
        results = []
        cant_model_name= 'count' if modelo=="top_product_df" else 'ctr'
        for row in rows:
  
            results.append({ 
                'product_id': row[0],
                'model':modelo,
                f'{cant_model_name}':row[1]
            }) 

    # Retornar los resultados en formato JSON
        return results
    
    finally:
        cursor.close()
# Este bloque permite ejecutar la aplicación cuando se llama al archivo directamente
if __name__ == "__main__":
    uvicorn.run("main2:app", 
                host="0.0.0.0",
                port=int(os.getenv('PORT', 8080)),
                proxy_headers=True)
