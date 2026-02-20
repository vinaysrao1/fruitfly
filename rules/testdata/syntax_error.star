rule_id = "broken"
event_type = "post"
priority = 50

def evaluate(event)
    return verdict("approve")
