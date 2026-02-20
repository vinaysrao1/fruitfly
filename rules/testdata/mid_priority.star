rule_id = "mid-priority"
event_type = "comment"
priority = 100

def evaluate(event):
    return verdict("review", reason="needs review")
