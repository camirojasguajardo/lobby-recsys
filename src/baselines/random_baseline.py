import pandas as pd
import os

active_subjects = pd.read_csv(os.path.join("data", "active_subjects.csv"))
audiences = pd.read_csv(os.path.join("data", 'audiencies.csv'))
institutions = pd.read_csv(os.path.join("data", 'institutions.csv'))
passive_subjects = pd.read_csv(os.path.join("data", 'passive_subjects.csv'))


df = active_subjects[["sujeto_pasivo_id", "Nombre completo"]].copy()


import numpy as np
from sklearn.metrics import accuracy_score

df = active_subjects[["sujeto_pasivo_id", "Nombre completo"]].copy()

# espacio de etiquetas
nombres_unicos = df["Nombre completo"].unique()

# función para predecir aleatoriamente
def random_predict(df, nombres_unicos, seed=None):
    if seed is not None:
        np.random.seed(seed)
    return np.random.choice(nombres_unicos, size=len(df))

# generar predicciones
y_true = df["Nombre completo"].values
y_pred = random_predict(df, nombres_unicos, seed=42)

# medir precisión
precision = accuracy_score(y_true, y_pred)

print("Precisión del modelo aleatorio:", precision)
