import pandas as pd
import os
from sklearn.metrics import accuracy_score


active_subjects = pd.read_csv(os.path.join("data", "active_subjects.csv"))
audiences = pd.read_csv(os.path.join("data", 'audiencies.csv'))
institutions = pd.read_csv(os.path.join("data", 'institutions.csv'))
passive_subjects = pd.read_csv(os.path.join("data", 'passive_subjects.csv'))

df = active_subjects[["sujeto_pasivo_id", "Nombre completo"]].copy()


def most_popular_by_id(df):
    # calcular el nombre más frecuente para cada sujeto_pasivo_id
    popular_map = (
        df.groupby("sujeto_pasivo_id")["Nombre completo"]
        .agg(lambda x: x.value_counts().idxmax())
        .to_dict()
    )

    # predecir según el sujeto_pasivo_id de cada fila
    y_pred = df["sujeto_pasivo_id"].map(popular_map).values
    return y_pred

# uso
y_true = df["Nombre completo"].values
y_pred = most_popular_by_id(df)

precision = accuracy_score(y_true, y_pred)
print("Precisión del modelo Most Popular por ID:", precision)