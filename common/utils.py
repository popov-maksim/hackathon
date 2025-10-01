import ast
import unicodedata
from collections import defaultdict
from typing import List, Dict, Tuple


def nfc(s: str) -> str:
    return unicodedata.normalize("NFC", s)


def normalize_pred(obj) -> List[Dict] | None:
    try:
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

        return None
    except Exception:
        return None


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
    entity_types = set()

    for gold, pred in samples:
        if gold:
            entity_types.update({s["label"][2:] for s in gold})
        if pred:
            entity_types.update({s["label"][2:] for s in pred})

    tp_per_type = defaultdict(int)
    fp_per_type = defaultdict(int)
    fn_per_type = defaultdict(int)

    for gold, pred in samples:
        gold_by_type = defaultdict(set)
        pred_by_type = defaultdict(set)

        for s in gold:
            gold_by_type[s["label"][2:]].add((s["start"], s["end"], s["label"]))

        for s in pred:
            pred_by_type[s["label"][2:]].add((s["start"], s["end"], s["label"]))

        for t in entity_types:
            gold_set = gold_by_type[t]
            pred_set = pred_by_type[t]

            tp_per_type[t] += len(gold_set & pred_set)
            fp_per_type[t] += len(pred_set - gold_set)
            fn_per_type[t] += len(gold_set - pred_set)

    f1_scores = []
    for t in entity_types:
        tp = tp_per_type[t]
        fp = fp_per_type[t]
        fn = fn_per_type[t]

        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

        f1_scores.append(f1)

    macro_f1 = sum(f1_scores) / len(f1_scores) if f1_scores else 0
    return macro_f1
