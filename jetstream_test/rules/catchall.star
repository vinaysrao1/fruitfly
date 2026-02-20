rule_id = "catchall-approve"
event_type = "*"
priority = 1

def evaluate(event):
    return verdict("approve")
