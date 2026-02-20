rule_id = "numeric-content"
event_type = "post"
priority = 90

def evaluate(event):
    text = event["payload"].get("text", "")

    numeric_count = 0
    for ch in text.elems():
        if ch >= "0" and ch <= "9":
            numeric_count += 1

    if numeric_count > 10:
        return verdict("review", reason="high numeric content: " + str(numeric_count) + " digits")

    return verdict("approve")
