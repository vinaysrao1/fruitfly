rule_id = "wildcard-rule"
event_type = "*"
priority = 10

def evaluate(event):
    return verdict("approve")
