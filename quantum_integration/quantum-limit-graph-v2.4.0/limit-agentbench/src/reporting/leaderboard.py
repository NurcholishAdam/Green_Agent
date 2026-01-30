def generate_leaderboard(results):
    valid = [r for r in results if r["policy"]["compliant"]]

    return sorted(
        valid,
        key=lambda r: (-r["accuracy"], r["energy"], r["carbon"])
    )
