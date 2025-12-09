# -*- coding: utf-8 -*-
"""
LightGCN tripartito (usuarios, items, temas) con BPR,
para usar directamente desde un cuaderno con DataFrames.

Requiere:
- interactions_df: columnas [user_id, item_id, timestamp]
- item_topics_df: columnas [item_id, topic_id]
- topics_df: columnas [topic_id, embedding]  (embedding = np.ndarray 1D)
"""

import math
import random
from collections import defaultdict
from typing import Dict, Iterable, Tuple, Optional, List

import numpy as np
import pandas as pd
import torch
from torch import nn


# ------------------------ Métricas ------------------------
def dcg_at_k(rel, k):
    rel = np.asfarray(rel)[:k]
    if rel.size == 0:
        return 0.0
    return np.sum(rel / np.log2(np.arange(2, rel.size + 2)))


def ndcg_at_k(rel, k):
    dcg = dcg_at_k(rel, k)
    ideal = dcg_at_k(sorted(rel, reverse=True), k)
    return dcg / ideal if ideal > 0 else 0.0


def average_precision_at_k(rel, k):
    rel = np.asarray(rel, dtype=int)[:k]
    if rel.sum() == 0:
        return 0.0
    precisions, hits = [], 0
    for i, r in enumerate(rel, 1):
        if r:
            hits += 1
            precisions.append(hits / i)
    return np.mean(precisions) if precisions else 0.0


# ------------------------ Splits temporales ------------------------
def make_temporal_splits(df: pd.DataFrame) -> pd.DataFrame:
    """
    Recibe df con columnas: user_id, item_id, timestamp (datetime o str)
    Devuelve el mismo df con columna 'split' en {train,val,test}
    usando leave-last-1 por usuario (última = test, penúltima = val).
    """
    assert {"user_id", "item_id", "timestamp"}.issubset(df.columns)
    out = df.copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce")
    out = out.dropna(subset=["user_id", "item_id", "timestamp"]).sort_values(
        ["user_id", "timestamp"]
    )
    last = out.groupby("user_id").tail(1).assign(split="test")
    rest = out.drop(last.index)
    val = rest.groupby("user_id").tail(1).assign(split="val")
    train = rest.drop(val.index).assign(split="train")
    return pd.concat([train, val, last], ignore_index=True)


# ------------------------ Grafo tripartito ------------------------
def build_A_hat_tripartite(
    n_users: int,
    n_items: int,
    n_topics: int,
    ui_pairs: np.ndarray,
    it_pairs: np.ndarray,
) -> torch.Tensor:
    """
    Construye Â = D^{-1/2} A D^{-1/2} como tensor disperso COO (cuda/mps/cpu).

    Nodos globales:
      usuarios:   0 .. n_users-1
      items:      n_users .. n_users+n_items-1
      topics:     n_users+n_items .. n_users+n_items+n_topics-1

    ui_pairs: (E_ui, 2)  -> (u, i)  con IDs 0-based
    it_pairs: (E_it, 2)  -> (i, t)  con IDs 0-based
    """
    rows, cols = [], []

    # 1) U - I edges (bipartito usuario-item)
    for u, i in ui_pairs:
        u_g = int(u)
        a_g = n_users + int(i)
        rows.extend([u_g, a_g])
        cols.extend([a_g, u_g])

    # 2) I - T edges (item-topic)
    for i, t in it_pairs:
        a_g = n_users + int(i)
        t_g = n_users + n_items + int(t)
        rows.extend([a_g, t_g])
        cols.extend([t_g, a_g])

    idx = torch.tensor([rows, cols], dtype=torch.long)
    vals = torch.ones(len(rows), dtype=torch.float32)
    N = n_users + n_items + n_topics
    A = torch.sparse_coo_tensor(idx, vals, size=(N, N)).coalesce()

    # normalización por grado: D^{-1/2} A D^{-1/2}
    deg = torch.sparse.sum(A, dim=1).to_dense()
    deg[deg == 0] = 1.0
    d_inv_sqrt = torch.pow(deg, -0.5)
    D_idx = torch.arange(N)
    D_inv_sqrt = torch.sparse_coo_tensor(
        torch.stack([D_idx, D_idx]), d_inv_sqrt, (N, N)
    ).coalesce()

    A_hat = torch.sparse.mm(torch.sparse.mm(D_inv_sqrt, A), D_inv_sqrt).coalesce()
    return A_hat


# ------------------------ Modelo tripartito ------------------------
class TripartiteLightGCN(nn.Module):
    def __init__(
        self,
        n_users: int,
        n_items: int,
        n_topics: int,
        dim: int,
        n_layers: int,
        A_hat: torch.Tensor,
        topic_init: Optional[torch.Tensor] = None,
    ):
        """
        n_users, n_items, n_topics: conteos
        A_hat: matriz de adyacencia normalizada (sparse)
        topic_init: tensor (n_topics, dim) con embeddings iniciales de temas (opcional)
        """
        super().__init__()
        self.n_users = n_users
        self.n_items = n_items
        self.n_topics = n_topics
        self.N = n_users + n_items + n_topics
        self.dim = dim
        self.n_layers = n_layers
        self.A_hat = A_hat.coalesce()

        # Embedding único para TODOS los nodos
        self.node_emb = nn.Embedding(self.N, dim)
        nn.init.uniform_(self.node_emb.weight, a=-0.005, b=0.005)

        # Inicializar temas con embeddings semánticos, si se entregan
        if topic_init is not None:
            assert topic_init.shape == (n_topics, dim), (
                f"topic_init debe ser de shape ({n_topics}, {dim}), "
                f"pero es {topic_init.shape}"
            )
            with torch.no_grad():
                start = n_users + n_items
                self.node_emb.weight[start : start + n_topics].copy_(topic_init)

    def propagate(self):
        """
        Propaga LightGCN sobre el grafo tripartito y retorna:
          users_final, items_final, topics_final
        """
        E = self.node_emb.weight  # (N, dim)
        outs = [E]
        for _ in range(self.n_layers):
            E = torch.sparse.mm(self.A_hat, E)
            outs.append(E)
        E_final = torch.stack(outs, dim=0).mean(dim=0)

        users_final = E_final[: self.n_users]
        items_final = E_final[self.n_users : self.n_users + self.n_items]
        topics_final = E_final[self.n_users + self.n_items :]
        return users_final, items_final, topics_final

    def forward(self):
        return self.propagate()

    def score(
        self,
        user_ids: torch.Tensor,
        item_ids: torch.Tensor,
        users_final: Optional[torch.Tensor] = None,
        items_final: Optional[torch.Tensor] = None,
    ) -> torch.Tensor:
        """
        Score usuario-item: producto punto entre embeddings finales.
        """
        if users_final is None or items_final is None:
            users_final, items_final, _ = self.propagate()
        return (users_final[user_ids] * items_final[item_ids]).sum(dim=1)


# ------------------------ Sampler y evaluación ------------------------
def to_user_pos_dict(pairs: np.ndarray) -> Dict[int, set]:
    d = defaultdict(set)
    for u, i in pairs:
        d[int(u)].add(int(i))
    return d


def sample_bpr(
    tr_ui: Dict[int, set], n_items: int, n_samples: int
) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    """
    Muestra (u, i_pos, i_neg) para BPR con negativos uniformes.
    """
    users = list(tr_ui.keys())
    u_batch, ip_batch, in_batch = [], [], []
    for _ in range(n_samples):
        u = random.choice(users)
        i_pos = random.choice(tuple(tr_ui[u]))
        # negativo uniforme
        while True:
            j = random.randrange(n_items)
            if j not in tr_ui[u]:
                break
        u_batch.append(u)
        ip_batch.append(i_pos)
        in_batch.append(j)
    return (torch.tensor(u_batch), torch.tensor(ip_batch), torch.tensor(in_batch))


@torch.no_grad()
def evaluate_allranking(
    model: TripartiteLightGCN,
    tr_ui: Dict[int, set],
    te_ui: Dict[int, set],
    n_users: int,
    n_items: int,
    ks: Iterable[int] = (10, 20),
    device: Optional[str] = None,
) -> Dict[str, float]:
    """
    Evalúa ranking usuario->items en conjunto de test (o val).
    """
    device = device or ("cuda" if torch.cuda.is_available() else "cpu")
    outs = model.propagate()
    if len(outs) == 2:
        users_f, items_f = outs
    else:
        users_f, items_f, _ = outs

    users_f = users_f.to(device)
    items_f = items_f.to(device)

    recalls = {k: [] for k in ks}
    ndcgs = {k: [] for k in ks}
    maps = {k: [] for k in ks}

    for u, pos_set in te_ui.items():
        if not pos_set:
            continue
        scores = (users_f[u] * items_f).sum(dim=1)

        # filtra vistos en train
        if u in tr_ui and len(tr_ui[u]) > 0:
            seen = torch.tensor(list(tr_ui[u]), device=device, dtype=torch.long)
            scores[seen] = -1e9

        rank_idx = torch.argsort(scores, descending=True).cpu().numpy()
        rel = np.isin(rank_idx, list(pos_set)).astype(int)

        for k in ks:
            hits = rel[:k].sum()
            recalls[k].append(hits / max(1, len(pos_set)))
            ndcgs[k].append(ndcg_at_k(rel, k))
            maps[k].append(average_precision_at_k(rel, k))

    out = {f"Recall@{k}": float(np.mean(recalls[k])) for k in ks}
    out.update({f"nDCG@{k}": float(np.mean(ndcgs[k])) for k in ks})
    out.update({f"MAP@{k}": float(np.mean(maps[k])) for k in ks})
    return out


# ------------------------ Runner tripartito ------------------------
class TripartiteLightGCNRunner:
    def __init__(
        self,
        dim: int = 64,
        n_layers: int = 3,
        lr: float = 1e-3,
        l2: float = 1e-4,
        batch_size: int = 4096,
        epochs: int = 300,
        patience: int = 20,
        device: Optional[str] = None,
        seed: int = 42,
    ):
        # device
        if device is not None:
            self.device = device
        else:
            if torch.backends.mps.is_available():
                self.device = "mps"
            elif torch.cuda.is_available():
                self.device = "cuda"
            else:
                self.device = "cpu"

        self.dim = dim
        self.n_layers = n_layers
        self.lr = lr
        self.l2 = l2
        self.batch_size = batch_size
        self.epochs = epochs
        self.patience = patience
        self.seed = seed

        # artefactos
        self.model: Optional[TripartiteLightGCN] = None
        self.tr_ui: Optional[Dict[int, set]] = None
        self.va_ui: Optional[Dict[int, set]] = None
        self.te_ui: Optional[Dict[int, set]] = None
        self.n_users: Optional[int] = None
        self.n_items: Optional[int] = None
        self.n_topics: Optional[int] = None
        self.best_state = None

        random.seed(seed)
        np.random.seed(seed)
        torch.manual_seed(seed)
        if self.device in ("cuda", "mps"):
            torch.cuda.manual_seed_all(seed)

    def _train_loop(
        self,
        A_hat: torch.Tensor,
    ):
        self.model = TripartiteLightGCN(
            self.n_users,
            self.n_items,
            self.n_topics,
            self.dim,
            self.n_layers,
            A_hat,
            topic_init=self.topic_init_tensor,
        ).to(self.device)

        opt = torch.optim.Adam(self.model.parameters(), lr=self.lr, weight_decay=0.0)

        best_val = -1.0
        wait = 0
        tr_pairs = np.array(
            [(u, i) for u, items in self.tr_ui.items() for i in items],
            dtype=np.int64,
        )

        for epoch in range(1, self.epochs + 1):
            self.model.train()
            n_pairs = max(len(tr_pairs), self.batch_size)
            steps = math.ceil(n_pairs / self.batch_size)
            losses = []

            for step in range(steps):
                u, ip, ineg = sample_bpr(self.tr_ui, self.n_items, self.batch_size)
                u, ip, ineg = (
                    u.to(self.device),
                    ip.to(self.device),
                    ineg.to(self.device),
                )

                users_f, items_f, topics_f = self.model.propagate()

                x_pos = (users_f[u] * items_f[ip]).sum(dim=1)
                x_neg = (users_f[u] * items_f[ineg]).sum(dim=1)
                loss_bpr = -torch.log(torch.sigmoid(x_pos - x_neg) + 1e-12).mean()

                # regularización L2 sobre embeddings de nodos usados (usuarios + items pos/neg)
                u_glob = u
                ip_glob = self.n_users + ip
                in_glob = self.n_users + ineg
                loss_reg = (
                    self.l2
                    * (
                        self.model.node_emb(u_glob).pow(2).sum()
                        + self.model.node_emb(ip_glob).pow(2).sum()
                        + self.model.node_emb(in_glob).pow(2).sum()
                    )
                    / u.shape[0]
                )

                loss = loss_bpr + loss_reg

                opt.zero_grad()
                loss.backward()
                opt.step()
                losses.append(loss.item())

            # --- validación ---
            self.model.eval()
            val_metrics = evaluate_allranking(
                self.model,
                self.tr_ui,
                self.va_ui,
                self.n_users,
                self.n_items,
                ks=(10, 20),
                device=self.device,
            )
            val_score = val_metrics["nDCG@10"]
            print(
                f"Epoch {epoch:03d} | loss {np.mean(losses):.4f} | "
                f"val nDCG@10 {val_score:.4f}"
            )

            if val_score > best_val + 1e-5:
                best_val, wait = val_score, 0
                self.best_state = {
                    k: v.detach().cpu().clone()
                    for k, v in self.model.state_dict().items()
                }
            else:
                wait += 1
                if wait >= self.patience:
                    print("Early stopping.")
                    break

        if self.best_state is not None:
            self.model.load_state_dict(self.best_state)

    # -------- API pública principal ----------
    def fit(
        self,
        interactions_df: pd.DataFrame,
        item_topics_df: pd.DataFrame,
        topics_df: pd.DataFrame,
    ) -> Dict[str, float]:
        """
        Entrena el modelo tripartito.

        interactions_df:
            columnas: user_id (0-based), item_id (0-based), timestamp

        item_topics_df:
            columnas: item_id (0-based), topic_id (0-based)

        topics_df:
            columnas: topic_id (0-based), embedding (np.ndarray 1D de largo dim)
        """
        # 1) Splits temporales U-I
        splits = make_temporal_splits(interactions_df)
        train_df = splits[splits.split == "train"][["user_id", "item_id"]]
        val_df = splits[splits.split == "val"][["user_id", "item_id"]]
        test_df = splits[splits.split == "test"][["user_id", "item_id"]]

        # 2) Conteos
        self.n_users = (
            int(
                max(
                    train_df["user_id"].max(),
                    val_df["user_id"].max(),
                    test_df["user_id"].max(),
                )
            )
            + 1
        )
        self.n_items = (
            int(
                max(
                    train_df["item_id"].max(),
                    val_df["item_id"].max(),
                    test_df["item_id"].max(),
                )
            )
            + 1
        )
        self.n_topics = int(item_topics_df["topic_id"].max()) + 1

        # 3) Pairs U-I (splits)
        tr_pairs = train_df[["user_id", "item_id"]].to_numpy(np.int64)
        va_pairs = val_df[["user_id", "item_id"]].to_numpy(np.int64)
        te_pairs = test_df[["user_id", "item_id"]].to_numpy(np.int64)

        self.tr_ui = to_user_pos_dict(tr_pairs)
        self.va_ui = to_user_pos_dict(va_pairs)
        self.te_ui = to_user_pos_dict(te_pairs)

        # 4) Pairs I-T (a partir de item_topics_df)
        it_pairs = (
            item_topics_df[["item_id", "topic_id"]]
            .dropna()
            .drop_duplicates()
            .to_numpy(np.int64)
        )

        # 5) topic_init desde topics_df
        topics_sorted = topics_df.sort_values("topic_id")
        topic_emb_list = topics_sorted["embedding"].values
        topic_init = np.stack(topic_emb_list, axis=0).astype("float32")
        assert (
            topic_init.shape[0] == self.n_topics
        ), f"n_topics={self.n_topics} pero topic_init tiene {topic_init.shape[0]} filas"
        self.topic_init_tensor = torch.from_numpy(topic_init)

        # 6) Construir A_hat tripartito
        A_hat = build_A_hat_tripartite(
            self.n_users, self.n_items, self.n_topics, tr_pairs, it_pairs
        ).to(self.device)

        # 7) Entrenar
        self._train_loop(A_hat)

        # 8) Métricas test
        self.model.eval()
        test_metrics = evaluate_allranking(
            self.model,
            self.tr_ui,
            self.te_ui,
            self.n_users,
            self.n_items,
            ks=(10, 20),
            device=self.device,
        )
        print("== TEST ==")
        for k, v in test_metrics.items():
            print(f"{k}: {v:.4f}")
        return test_metrics

    @torch.no_grad()
    def recommend(
        self, user_id: int, topk: int = 20, filter_seen: bool = True
    ) -> List[int]:
        """
        Retorna lista de item_ids recomendados para user_id.
        Requiere haber llamado fit.
        """
        assert self.model is not None, "Entrena el modelo primero."
        users_f, items_f, topics_f = self.model.propagate()
        users_f = users_f.to(self.device)
        items_f = items_f.to(self.device)

        scores = (users_f[user_id] * items_f).sum(dim=1)
        if filter_seen and self.tr_ui and user_id in self.tr_ui:
            seen = torch.tensor(
                list(self.tr_ui[user_id]), device=self.device, dtype=torch.long
            )
            scores[seen] = -1e9
        top_idx = torch.argsort(scores, descending=True)[:topk]
        return top_idx.detach().cpu().tolist()
