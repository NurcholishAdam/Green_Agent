from typing import Dict, List


def generate_policy_report(
    all_results: List[Dict],
    policy_hash: str,
) -> Dict:
    compliant = [r for r in all_results if r["policy"]["compliant"]]
    violated = [r for r in all_results if not r["policy"]["compliant"]]

    return {
        "policy_hash": policy_hash,
        "total_runs": len(all_results),
        "compliant_runs": len(compliant),
        "violated_runs": len(violated),
        "compliance_rate": (
            len(compliant) / len(all_results) if all_results else 0.0
        ),
        "results": all_results,
    }
