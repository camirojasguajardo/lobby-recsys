# 🏛️ LOBBY-RECSYS  
### Sistema de recomendación para la Ley de Lobby (Chile)

Este repositorio implementa un sistema de recomendación diseñado para facilitar la participación ciudadana bajo la **Ley N° 20.730**, ayudando a identificar qué autoridades públicas (*sujetos pasivos*) son pertinentes según los intereses temáticos y el historial de los solicitantes (*sujetos activos*).

El sistema combina técnicas de **representación semántica**, **recomendación colaborativa**, **clustering temático** y **grafos neuronales**, con el fin de capturar afinidad temática en un dominio caracterizado por esparsidad extrema, arranque en frío (*cold-start*) y fuerte sesgo de popularidad.

---

## 📁 Estructura del repositorio

```
LOBBY-RECSYS/
│
├── data/                     # Datos en https://drive.google.com/drive/folders/1RF51mMLP0NgEeuKdd9ZT0fuUHAM8tK0Y?usp=share_link
│
├── src/
│
├── baselines/                 
│   ├── ItemKNN.ipynb          # Implementación y evaluación del modelo Item-KNN
│   ├── UserKNN.ipynb          # Implementación y evaluación del modelo User-KNN
│   └── ALS.ipynb              # Factorización Matricial (ALS) para feedback implícito
│   # → Contiene los modelos de referencia que sirven como línea base comparativa.
│
├── scrapping/
│   └── ...                    # Scripts para extraer audiencias desde leylobby.gob.cl
│   # → Automatiza la descarga, limpieza inicial y estructuración de las audiencias.
│
├── state_of_art_models/
│   ├── content_bert.ipynb         # Generación de embeddings con SBERT
│   ├── semantic_clustering.ipynb  # Pipeline completo de clustering (HDBSCAN + KMeans)
│   ├── lightGCN_base.py           # Implementación modular del modelo LightGCN
│   ├── lightGCN.ipynb             # Entrenamiento del LightGCN base sobre el dataset completo
│   ├── tripartite_lightGCN.py     # Versión ampliada del modelo con nodos temáticos
│   ├── tripartite_lightGCN.ipynb  # Entrenamiento end-to-end del modelo tripartito
│   ├── ItemKNN.ipynb              # Entrenamiento y validación del modelo Item-KNN
│   └── userKNN.ipynb              # Entrenamiento y validación del modelo User-KNN
│   # → Contiene notebooks experimentales y versiones modulares de los modelos SOTA.
│   #   Aquí se ejecutan los entrenamientos principales del proyecto.
│
├── evaluation.py               # Funciones estandarizadas para evaluación (Recall, MAP, nDCG)
├── metrics.py                  # Implementación detallada de métricas top-k y medidas de equidad (Gini)
├── utils.py                    # Utilidades generales: carga de datos, manipulación de grafos, helpers
│
├── data_loading.ipynb        # Limpieza y estructura del dataset
├── data_analysis.ipynb       # Exploración y estadísticas descriptivas
├── text.ipynb                # Generación de embeddings SBERT
├── lightfm.ipynb             # Implementación LightFM (WARP)
├── lightGCN.ipynb            # Entrenamiento LightGCN completo
├── tripartite_lightGCN.ipynb # Experimentos del modelo tripartito
│
├── LICENSE
└── README.md
```

---

## 🧠 Metodología y componentes principales

### 1) **Embeddings semánticos**
Los textos del campo *detalle* son representados mediante:

- `distiluse-base-multilingual-cased-v2`
- Reducción de dimensionalidad con **PCA**
- Normalización previa al clustering y uso en modelos

Estos embeddings permiten capturar afinidad temática sin depender de coincidencias literales entre palabras.

---

### 2) **Clustering Temático (Híbrido: HDBSCAN + K-Means)**

Dado que los textos son breves y dispersos, se utiliza un enfoque de dos etapas:

1. **HDBSCAN** para identificar núcleos densos y coherentes.
2. **MiniBatch K-Means** para recuperar el resto del corpus (outliers).

El resultado final agrupa más de **2.000 temas latentes**, posteriormente utilizados para construir perfiles semánticos de usuarios y autoridades.

---

### 3) **Modelos de recomendación implementados**

#### 🔹 *Baselines*  
- **Item-KNN**, **User-KNN**, **ALS**  
- Permiten evaluar el aporte incremental de los modelos avanzados.  

#### 🔹 **LightFM (WARP)**  
- Combina señales colaborativas + embeddings semánticos.  
- Ideal para escenarios con texto altamente informativo.

#### 🔹 **LightGCN (Base)**  
- Modelo de grafos bipartito Usuario–Autoridad.  
- Entrenamiento con pérdida **BPR**.

#### 🔹 **LightGCN Tripartito (Propuesto)**  
- Extiende el grafo incorporando nodos temáticos.  
- Permite integrar estructura colaborativa + semántica.  

#### 🔹 **Clustering Semántico como Recomendador**  
- Estrategia basada únicamente en similitud coseno entre embeddings.  
- En evaluaciones internas obtuvo el **mejor recall y nDCG** del conjunto.
