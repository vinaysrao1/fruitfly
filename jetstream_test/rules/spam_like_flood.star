rule_id = "spam-like-flood"
event_type = "like"
priority = 100

def evaluate(event):
    entity_id = event["payload"].get("entity_id", "")

    count = counter(entity_id, "like", 600)
    if count > 5:
        return verdict("block", reason="spam: like flood (" + str(count) + " likes in 10min)")

    return verdict("approve")
