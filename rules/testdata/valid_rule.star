rule_id = "test-valid-rule"
event_type = "post"
priority = 100

def evaluate(event):
    return verdict("approve")
