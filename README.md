## 📘 Descripción general

Un sistema de recomendación que busca facilitar la participación ciudadana en el marco de la **Ley N° 20.730**, ayudando a identificar qué autoridades (*sujetos pasivos*) son más pertinentes según el perfil y los intereses temáticos de quienes solicitan audiencias (*sujetos activos*).

El sistema combina enfoques **híbridos** (basados en contenido y feedback implícito) y **basados en grafos**, para generar recomendaciones informadas y coherentes con las competencias institucionales.

---

## 🧩 Estructura del repositorio

LOBBY-RECSYS/
│
├── data/ # Datos base extraídos desde leylobby.gob.cl
│ ├── active_subjects.csv
│ ├── passive_subjects.csv
│ ├── audiencies.csv
│ ├── institutions.csv
│ └── materias_embeddings.parquet
│
├── output/ # Resultados y modelos entrenados
│ ├── recs_top20.csv / .json
│ └── lightGCN/
│ ├── models/
│ ├── recommendations_top10/
│ └── training_log/
│
├── src/ # Código fuente principal
│ ├── scrapping/ # Extracción desde el portal de lobby
│ └── state_of_art_models/
│ ├── lightFM.ipynb
│ ├── lightGCN.ipynb
│ ├── lightGCN_base.py
│ └── evaluation.py / metrics.py
│
├── data_loading.ipynb # Limpieza y preprocesamiento
├── data_analysis.ipynb # Análisis exploratorio
├── text.ipynb # Generación de embeddings semánticos
└── README.md


---

## ⚙️ Modelos implementados

### 🔹 LightFM — Híbrido basado en contenido
- Combina interacciones históricas con *embeddings* semánticos derivados de las materias tratadas.  
- Embeddings generados con `sentence-transformers/distiluse-base-multilingual-cased-v2` (384 dims).  
- **Loss:** WARP  
- **Métricas:** Precision@10, AUC  
- Exploración futura: uso de **DeepFM** para modelar interacciones no lineales y mejorar la precisión top-k.  

---

### 🔹 LightGCN — Basado en grafos
- Modelo bipartito de sujetos activos ↔ pasivos, entrenado con pérdida **BPR**.  
- **300 épocas**, **64 dimensiones**, **3 capas de propagación**.  
- Convergencia estable: *nDCG@10 ≈ 0.047*, *Recall@10 ≈ 0.024*.  
- Extensión propuesta: **LightGCN Tripartito**, incorporando nodos temáticos inicializados con embeddings semánticos.  

---

### 🔹 2-Stage (Clustering + Light/DeepFM)
Estrategia jerárquica:
1. Agrupamiento semántico de autoridades (clustering de embeddings).  
2. Re-ranking supervisado con LightFM o DeepFM.  

Permite reducir el espacio de búsqueda, mejorar la precisión local y mitigar el problema de *cold-start*.  

---

## 🚀 Próximos pasos

- Implementar **DeepFM** con variables semánticas e institucionales mixtas.  
- Desarrollar **LightGCN Tripartito** con nodos temáticos.  
- Evaluar comparativamente Recall@k, nDCG@k y MAP.  
- Elaborar el **póster y artículo final** del proyecto.

---

## 📚 Referencias

- He et al. (2020). *LightGCN: Simplifying and Powering Graph Convolution Network for Recommendation.*  
- Guo et al. (2017). *DeepFM: A Factorization-Machine based Neural Network for CTR Prediction.*  
- Rendle (2010). *Factorization Machines.*  
- Reimers & Gurevych (2019). *Sentence-Transformers.*  

---
