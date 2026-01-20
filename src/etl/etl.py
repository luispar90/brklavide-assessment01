from datetime import datetime
from typing import Any

from src.etl.utils import stable_hash


class ObrasETL:
    """
    ETL simple e idempotente.
    - Extrae: generos, filmes por genero, detail filme, avaliacoes
    - Carga: generos, filmes, filmes_generos, avaliacoes
    """

    def __init__(self, api, repo):
        self.api = api
        self.repo = repo

    def run(self) -> None:

        # 1) API de Generos
        generos_payload = self.api.get_json("/obras/v3/generos")
        genero_rows = self._transform_generos(generos_payload)
        self.repo.upsert("generos", genero_rows, ["genero_id"])

        # 2) Filmes por genero + bridge
        filme_ids: set[int] = set()
        bridge_rows: list[dict[str, Any]] = []

        for g in genero_rows:
            gid = g["genero_id"]
            filmes_list = self.api.get_json(f"/obras/v3/generos/{gid}/filmes")
            for f in filmes_list:
                fid = f.get("idFilme") or f.get("id") or f.get("filmeId")
                if fid is None:
                    continue
                fid = int(fid)
                filme_ids.add(fid)
                bridge_rows.append({"filme_id": fid, "genero_id": gid})

        self.repo.upsert("filmes_generos", bridge_rows, ["filme_id", "genero_id"])

        # 3) Detail filme + avaliacoes
        filmes_rows: list[dict[str, Any]] = []
        aval_rows: list[dict[str, Any]] = []

        for fid in filme_ids:
            detail = self.api.get_json(f"/obras/v3/filmes/{fid}")
            filmes_rows.append(self._transform_filme_detail(fid, detail))

            avs = self.api.get_json(f"/obras/v3/filmes/{fid}/avaliacoes")
            aval_rows.extend(self._transform_avaliacoes(fid, avs))

        self.repo.upsert("filmes", filmes_rows, ["filme_id"])
        self.repo.upsert("avaliacoes", aval_rows, ["avaliacao_id"])

    def _transform_generos(self, payload_list: list[dict]) -> list[dict]:
        rows = []
        for g in payload_list:
            gid = g.get("idGenero") or g.get("id") or g.get("generoId")
            if gid is None:
                continue
            rows.append({
                "genero_id": int(gid),
                "nome": g.get("nome") or g.get("name") or "unknown",
                "raw_payload": g,
                "updated_at": datetime.utcnow(),
            })
        return rows

    def _transform_filme_detail(self, fid: int, detail: dict) -> dict:
        return {
            "filme_id": fid,
            "titulo": detail.get("titulo") or detail.get("title"),
            "titulo_original": detail.get("tituloOriginal") or detail.get("originalTitle"),
            "ano": detail.get("ano") or detail.get("year"),
            "data_lancamento": detail.get("dataLancamento") or detail.get("releaseDate"),
            "duracao_min": detail.get("duracao") or detail.get("runtime"),
            "idioma": detail.get("idioma") or detail.get("language"),
            "sinopse": detail.get("sinopse") or detail.get("overview"),
            "raw_payload": detail,
            "updated_at": datetime.utcnow(),
        }

    def _transform_avaliacoes(self, filme_id: int, payload_list: list[dict]) -> list[dict]:
        rows = []
        for av in payload_list:
            avid = av.get("idAvaliacao") or av.get("id") or stable_hash({"filme_id": filme_id, **av})
            rows.append({
                "avaliacao_id": str(avid),
                "filme_id": filme_id,
                "nota": av.get("nota") or av.get("rating"),
                "autor": av.get("autor") or av.get("user"),
                "comentario": av.get("comentario") or av.get("comment"),
                "data_avaliacao": av.get("data") or av.get("createdAt"),
                "raw_payload": av,
            })
        return rows
