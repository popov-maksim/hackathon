import ast
import unicodedata
from typing import List, Dict, Tuple, Set


def nfc(s: str) -> str:
    return unicodedata.normalize("NFC", s)


def normalize_pred(obj) -> List[Dict]:
    try:
        if obj is None:
            return []
        if isinstance(obj, dict):
            if "spans" in obj:
                spans = obj.get("spans") or []
                out = []
                for it in spans:
                    if isinstance(it, dict) and {"start_index","end_index","entity"}.issubset(it.keys()):
                        out.append({"start": int(it["start_index"]), "end": int(it["end_index"]), "label": str(it["entity"])})
                return out
            if "annotation" in obj:
                ann = obj.get("annotation")
                if isinstance(ann, str):
                    return parse_annotation_literal(ann)
                if isinstance(ann, list):
                    tmp = []
                    for t in ann:
                        if isinstance(t, (list, tuple)) and len(t) == 3:
                            tmp.append({"start": int(t[0]), "end": int(t[1]), "label": str(t[2])})
                    return tmp
        if isinstance(obj, list):
            out = []
            for it in obj:
                if isinstance(it, dict) and {"start_index","end_index","entity"}.issubset(it.keys()):
                    out.append({"start": int(it["start_index"]), "end": int(it["end_index"]), "label": str(it["entity"])})
                elif isinstance(it, (list, tuple)) and len(it) == 3:
                    out.append({"start": int(it[0]), "end": int(it[1]), "label": str(it[2])})
            return out
    except Exception:
        return []
    return []


def parse_annotation_literal(s: str) -> List[Dict]:
    try:
        data = ast.literal_eval(s)
        result = []
        for tup in data:
            if not isinstance(tup, (list, tuple)) or len(tup) != 3:
                continue
            start, end, label = int(tup[0]), int(tup[1]), str(tup[2])
            if start < 0 or end <= start:
                continue
            result.append({"start": start, "end": end, "label": label})
        return result
    except Exception:
        return []


def f1_macro(samples: List[Tuple[List[Dict], List[Dict]]]) -> float:
    labels: Set[str] = set()
    for gold, _ in samples:
        for s in gold:
            labels.add(str(s["label"]))
    if not labels:
        return 0.0
    f1s = []
    for label in labels:
        TP = FP = FN = 0
        for gold, pred in samples:
            g = {(int(s["start"]), int(s["end"])) for s in gold if str(s["label"]) == label}
            p = {(int(s["start"]), int(s["end"])) for s in pred if str(s["label"]) == label}
            TP += len(g & p)
            FP += len(p - g)
            FN += len(g - p)
        precision = TP / (TP + FP) if (TP + FP) else 0.0
        recall = TP / (TP + FN) if (TP + FN) else 0.0
        f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
        f1s.append(f1)
    return sum(f1s) / len(f1s)
