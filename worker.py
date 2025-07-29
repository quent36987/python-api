import csv, io, os, tempfile, traceback
from typing import Any, List
from state import STATUS, Progress
import logging

LOCAL_TMP_ROOT = os.getenv("TMP_FILE")

# Début du code spécifique Python

def multiply_by_two(rows: List[List[float]]) -> List[List[float]]:
    return [[val * 2 for val in row] for row in rows]

def concat_matrices(matrices: List[List[List[Any]]]) -> List[List[Any]]:
    return [row for matrix in matrices for row in matrix]

def run_calculation(run_id: str,
                    input_csvs: List[str],
                    output_files: List[str]):
    try:
        # MAJ du status
        update_status(run_id, 0, False)

        # 1) Lecture individuelle + agrégation séparée
        matrices: List[List[List[Any]]] = [read_matrix_csv(path) for path in input_csvs]
        update_status(run_id, 10)

        # 2) Concaténation en une seule matrice List[List[Any]]
        data: List[List[Any]] = concat_matrices(matrices)
        update_status(run_id, 30)

        # 3) Ici, si vous voulez tout de même multiplier les valeurs numériques par deux :
        numeric_data = [
            [float(x) for x in row]  # conversion explicite
            for row in data
        ]
        doubled = multiply_by_two(numeric_data)
        update_status(run_id, 70)

        write_csv(run_id, output_files, doubled, ";")

        # 6) Marquer comme prêt (upload deferred)
        update_status(run_id, 100, True)

    except Exception as ex:
        update_status(run_id, 100, True, traceback.format_exc())
        # Vous pouvez aussi logger l'erreur ici si besoin
        return

## Fin du code spécifique Python

### Helper functions

def read_matrix_csv(path: str, delimiter: str = ";") -> List[List[Any]]:
    rows: List[List[Any]] = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f, delimiter=delimiter)
        for row in reader:
            # on ne convertit plus en float, on garde la valeur brute
            rows.append(row)  
    return rows

def update_status(run_id: str, progress: int, done: bool = False, error: str | None = None):
    STATUS[run_id] = Progress(progress=progress, done=done, error=error)

def write_csv(run_id: str, output_files: List[str], data: List[List[Any]], delimiter: str = ";") -> str:
    run_dir = os.path.join(LOCAL_TMP_ROOT, "output", run_id)
    os.makedirs(run_dir, exist_ok=True)

    written_paths: List[str] = []

    for of in output_files:
        # Ajouter l'extension si besoin
        filename = of if of.lower().endswith(".csv") else f"{of}.csv"
        out_path = os.path.join(run_dir, filename)

        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter=delimiter)
            writer.writerows(data)

        written_paths.append(out_path)

    return written_paths