# -*- coding: utf-8 -*-
# LightGCN "puro" (BPR) para usar directamente desde un cuaderno con DataFrames

import math, random
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


# ------------------------ Splits ------------------------
def make_temporal_splits(df: pd.DataFrame) -> pd.DataFrame:
    """
    Recibe df con columnas: user_id, item_id, timestamp (datetime o str)
    Devuelve el mismo df con columna 'split' en {train,val,test} usando leave-last-1 por usuario.
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


# ------------------------ Grafo ------------------------
def build_A_hat(n_users: int, n_items: int, ui_pairs: np.ndarray) -> torch.Tensor:
    """
    Construye Â = D^{-1/2} A D^{-1/2} como tensor disperso COO (cuda si disponible).
    ui_pairs: array shape (E,2) de (u,i) solo de TRAIN (IDs 0-based).
    """
    rows, cols = [], []
    for u, i in ui_pairs:
        rows.append(u)
        cols.append(n_users + i)
        rows.append(n_users + i)
        cols.append(u)

    idx = torch.tensor([rows, cols], dtype=torch.long)
    vals = torch.ones(len(rows), dtype=torch.float32)
    N = n_users + n_items
    A = torch.sparse_coo_tensor(idx, vals, size=(N, N)).coalesce()

    deg = torch.sparse.sum(A, dim=1).to_dense()
    deg[deg == 0] = 1.0
    d_inv_sqrt = torch.pow(deg, -0.5)
    D_idx = torch.arange(N)
    D_inv_sqrt = torch.sparse_coo_tensor(
        torch.stack([D_idx, D_idx]), d_inv_sqrt, (N, N)
    ).coalesce()

    A_hat = torch.sparse.mm(torch.sparse.mm(D_inv_sqrt, A), D_inv_sqrt).coalesce()
    return A_hat


# ------------------------ Modelo ------------------------
class LightGCN(nn.Module):
    def __init__(
        self, n_users: int, n_items: int, dim: int, n_layers: int, A_hat: torch.Tensor
    ):
        super().__init__()
        self.n_users = n_users
        self.n_items = n_items
        self.history = []
        self.dim = dim
        self.n_layers = n_layers
        self.A_hat = A_hat.coalesce()

        self.user_emb = nn.Embedding(n_users, dim)
        self.item_emb = nn.Embedding(n_items, dim)
        nn.init.uniform_(self.user_emb.weight, a=-0.005, b=0.005)
        nn.init.uniform_(self.item_emb.weight, a=-0.005, b=0.005)

    def propagate(self) -> Tuple[torch.Tensor, torch.Tensor]:
        E0 = torch.cat([self.user_emb.weight, self.item_emb.weight], dim=0)
        outs = [E0]
        E = E0
        for _ in range(self.n_layers):
            E = torch.sparse.mm(self.A_hat, E)
            outs.append(E)
        E_final = torch.stack(outs, dim=0).mean(dim=0)
        return E_final[: self.n_users], E_final[self.n_users :]

    def forward(self):
        return self.propagate()

    def score(
        self,
        user_ids: torch.Tensor,
        item_ids: torch.Tensor,
        users_final: Optional[torch.Tensor] = None,
        items_final: Optional[torch.Tensor] = None,
    ) -> torch.Tensor:
        if users_final is None or items_final is None:
            users_final, items_final = self.propagate()
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
    model: LightGCN,
    tr_ui: Dict[int, set],
    te_ui: Dict[int, set],
    n_users: int,
    n_items: int,
    ks: Iterable[int] = (10, 20),
    device: Optional[str] = None,
) -> Dict[str, float]:
    device = device or ("cuda" if torch.cuda.is_available() else "cpu")
    users_f, items_f = model.propagate()
    users_f = users_f.to(device)
    items_f = items_f.to(device)

    recalls = {k: [] for k in ks}
    ndcgs = {k: [] for k in ks}
    maps = {k: [] for k in ks}
    all_items = torch.arange(n_items, device=device)

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


# ------------------------ Runner para cuaderno ------------------------
class LightGCNRunner:
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
        self.dim = dim
        self.n_layers = n_layers
        self.lr = lr
        self.l2 = l2
        self.batch_size = batch_size
        self.epochs = epochs
        self.patience = patience
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        self.seed = seed

        # artefactos
        self.model: Optional[LightGCN] = None
        self.tr_ui: Optional[Dict[int, set]] = None
        self.va_ui: Optional[Dict[int, set]] = None
        self.te_ui: Optional[Dict[int, set]] = None
        self.n_users: Optional[int] = None
        self.n_items: Optional[int] = None
        self.best_state = None

        random.seed(seed)
        np.random.seed(seed)
        torch.manual_seed(seed)
        if self.device == "cuda":
            torch.cuda.manual_seed_all(seed)

    def _train_loop(
        self, A_hat: torch.Tensor, tr_pairs: np.ndarray, va_pairs: np.ndarray
    ):
        self.model = LightGCN(
            self.n_users, self.n_items, self.dim, self.n_layers, A_hat
        ).to(self.device)
        opt = torch.optim.Adam(self.model.parameters(), lr=self.lr, weight_decay=0.0)

        best_val = -1.0
        wait = 0
        for epoch in range(1, self.epochs + 1):
            self.model.train()
            n_pairs = max(len(tr_pairs), self.batch_size)
            steps = math.ceil(n_pairs / self.batch_size)
            losses = []

            for _ in range(steps):
                u, ip, ineg = sample_bpr(self.tr_ui, self.n_items, self.batch_size)
                u, ip, ineg = (
                    u.to(self.device),
                    ip.to(self.device),
                    ineg.to(self.device),
                )

                # 🔑 Propagate con gradiente habilitado (no .no_grad, no .to() extra)
                users_f, items_f = self.model.propagate()

                x_pos = (users_f[u] * items_f[ip]).sum(dim=1)
                x_neg = (users_f[u] * items_f[ineg]).sum(dim=1)
                loss_bpr = -torch.log(torch.sigmoid(x_pos - x_neg) + 1e-12).mean()
                loss_reg = (
                    self.l2
                    * (
                        self.model.user_emb(u).pow(2).sum()
                        + self.model.item_emb(ip).pow(2).sum()
                        + self.model.item_emb(ineg).pow(2).sum()
                    )
                    / u.shape[0]
                )
                loss = loss_bpr + loss_reg

                opt.zero_grad()
                loss.backward()
                opt.step()
                losses.append(loss.item())

            # --- validación (ok con no_grad dentro de evaluate_allranking) ---
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
                f"Epoch {epoch:03d} | loss {np.mean(losses):.4f} | val nDCG@10 {val_score:.4f}"
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

    # -------- APIs públicas ----------
    def fit(self, interactions_df: pd.DataFrame) -> Dict[str, float]:
        """
        Recibe df con columnas: user_id (int 0-based), item_id (int 0-based), timestamp
        Hace split temporal y entrena. Retorna métricas de test.
        """
        splits = make_temporal_splits(interactions_df)
        train_df = splits[splits.split == "train"][["user_id", "item_id"]]
        val_df = splits[splits.split == "val"][["user_id", "item_id"]]
        test_df = splits[splits.split == "test"][["user_id", "item_id"]]
        return self.fit_with_splits(train_df, val_df, test_df)

    def fit_with_splits(
        self, train_df: pd.DataFrame, val_df: pd.DataFrame, test_df: pd.DataFrame
    ) -> Dict[str, float]:
        """
        Entrena usando DataFrames ya separados (columnas: user_id,item_id)
        """
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

        tr_pairs = train_df[["user_id", "item_id"]].to_numpy(np.int64)
        va_pairs = val_df[["user_id", "item_id"]].to_numpy(np.int64)
        te_pairs = test_df[["user_id", "item_id"]].to_numpy(np.int64)

        self.tr_ui = to_user_pos_dict(tr_pairs)
        self.va_ui = to_user_pos_dict(va_pairs)
        self.te_ui = to_user_pos_dict(te_pairs)

        A_hat = build_A_hat(self.n_users, self.n_items, tr_pairs).to(self.device)
        self._train_loop(A_hat, tr_pairs, va_pairs)

        # métricas test
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
        [print(f"{k}: {v:.4f}") for k, v in test_metrics.items()]
        return test_metrics

    @torch.no_grad()
    def recommend(
        self, user_id: int, topk: int = 20, filter_seen: bool = True
    ) -> List[int]:
        """
        Retorna lista de item_ids recomendados para user_id.
        Requiere haber llamado fit/fit_with_splits.
        """
        assert self.model is not None, "Entrena el modelo primero."
        users_f, items_f = self.model.propagate()
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
