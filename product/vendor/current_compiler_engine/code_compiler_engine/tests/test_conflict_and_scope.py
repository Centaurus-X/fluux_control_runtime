from compiler_engine.domain.commit_kernel import resolve_conflicts
from compiler_engine.domain.sharding_decision import determine_rule_scope


def test_conflict_resolution_prefers_safety_stage() -> None:
    proposals = [
        {
            'proposal_id': 'P_LOW',
            'stage': 'control',
            'priority': 100,
            'deterministic_key': 'control|100|A',
            'conflict_group': 'signal:SIG_X',
            'payload': {'value': 1},
        },
        {
            'proposal_id': 'P_HIGH',
            'stage': 'safety',
            'priority': 10,
            'deterministic_key': 'safety|10|B',
            'conflict_group': 'signal:SIG_X',
            'payload': {'value': 0},
        },
    ]
    result = resolve_conflicts(proposals)
    winner_ref = result['conflict_groups'][0]['winning_proposal_ref']
    assert winner_ref == 'P_HIGH'


def test_sharding_decision_identifies_cross_physical_same_domain() -> None:
    rule = {
        'read_set': ['SIG_A'],
        'write_set': ['SIG_B'],
    }
    point_to_physical_context = {
        'POINT_A': 'POC_A',
        'POINT_B': 'POC_B',
    }
    signal_to_domain = {
        'SIG_A': 'LDC_1',
        'SIG_B': 'LDC_1',
    }
    signal_to_point = {
        'SIG_A': 'POINT_A',
        'SIG_B': 'POINT_B',
    }
    scope = determine_rule_scope(rule, point_to_physical_context, signal_to_domain, signal_to_point)
    assert scope == 'cross_physical_same_domain'
