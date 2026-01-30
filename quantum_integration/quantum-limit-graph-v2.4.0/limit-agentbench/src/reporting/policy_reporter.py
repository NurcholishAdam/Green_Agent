def generate_policy_report(results):
    return {
        "total": len(results),
        "compliant": len([r for r in results if r["policy"]["compliant"]]),
        "violated": len([r for r in results if not r["policy"]["compliant"]]),
    }
