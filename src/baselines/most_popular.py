import os
import pandas as pd
from sklearn.metrics import accuracy_score

active_subjects = pd.read_csv(os.path.join("data", "active_subjects.csv"))
audiences = pd.read_csv(os.path.join("data", 'audiencies.csv'))
institutions = pd.read_csv(os.path.join("data", 'institutions.csv'))
passive_subjects = pd.read_csv(os.path.join("data", 'passive_subjects.csv'))


df = active_subjects[["sujeto_pasivo_id", "Nombre completo"]].copy()


def most_popular_recommender(df):
    # encontrar el nombre más frecuente en todo el dataset
    most_popular = df["Nombre completo"].value_counts().idxmax()
    # predecir siempre ese nombre
    y_pred = [most_popular] * len(df)
    return y_pred

# uso
y_true = df["Nombre completo"].values
y_pred = most_popular_recommender(df)

precision = accuracy_score(y_true, y_pred)
print("Precisión del modelo Most Popular:", precision)

