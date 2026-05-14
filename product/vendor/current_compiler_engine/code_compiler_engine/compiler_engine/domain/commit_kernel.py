from typing import Any, Dict, List, Tuple


STAGE_ORDER = {
    "safety": 0,
    "guard": 1,
    "fsm": 2,
    "control": 3,
    "sequence": 4,
    "projection": 5,
    "notification": 6,
}


FSM_STATE_ORDER = {
    "INTERLOCK": 0,
    "FAULT": 1,
    "QUIESCING": 2,
    "RUNNING": 3,
    "IDLE": 4,
}


def proposal_sort_key(proposal: Dict[str, Any]) -> Tuple[int, int, str]:
    stage_rank = STAGE_ORDER.get(proposal.get("stage"), 999)
    priority = int(proposal.get("priority", 0))
    deterministic_key = str(proposal.get("deterministic_key", ""))
    return stage_rank, -priority, deterministic_key


def group_by_conflict(proposals: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    proposal: Dict[str, Any]
    for proposal in proposals:
        group = str(proposal.get("conflict_group", "__none__"))
        if group not in grouped:
            grouped[group] = []
        grouped[group].append(proposal)
    return grouped


def choose_winner(proposals: List[Dict[str, Any]]) -> Dict[str, Any]:
    ordered = sorted(proposals, key=proposal_sort_key)
    return ordered[0]


def resolve_conflicts(proposals: List[Dict[str, Any]]) -> Dict[str, Any]:
    grouped = group_by_conflict(proposals)
    results: List[Dict[str, Any]] = []
    conflict_group: str
    group_items: List[Dict[str, Any]]
    for conflict_group, group_items in sorted(grouped.items()):
        winner = choose_winner(group_items)
        losers = [item["proposal_id"] for item in group_items if item is not winner]
        results.append({
            "conflict_group": conflict_group,
            "winning_proposal_ref": winner["proposal_id"],
            "loser_proposal_refs": losers,
            "resolution_reason": "no_conflict" if not losers else "stage_precedence",
            "winner_payload": winner.get("payload", {}),
        })
    return {"conflict_groups": results}
