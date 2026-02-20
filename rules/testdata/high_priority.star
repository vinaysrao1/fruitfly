rule_id = "high-priority"
event_type = "post"
priority = 200

def evaluate(event):
    return verdict("block", reason="high priority block")
