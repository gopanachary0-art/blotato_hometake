"""The automation from the brief, as a definition a UI would produce."""

PRICING_AUTOMATION = {
    "trigger": {
        "type": "comment_created",
        "platform": "instagram",
        "scope": "any_post",
        "match": {"keywords": ["pricing"], "mode": "contains_word"},
    },
    "start": "s1",
    "steps": {
        "s1": {"type": "reply_to_comment", "text": "Sent you a DM!", "next": "s2"},
        "s2": {"type": "send_message",
               "text": "Hi {{trigger.username}}! What's the best email to send our pricing to?",
               "next": "s3"},
        "s3": {"type": "wait_for_reply", "validate": "email", "save_as": "email",
               "on_invalid": {"reprompt": "Hmm, that doesn't look like an email. Could you try again?",
                              "max_attempts": 2},
               "timeout": "23h",   # stay inside Instagram's 24h messaging window
               "on_timeout": "end",
               "next": "s4"},
        "s4": {"type": "send_message",
               "text": "Thanks! Here's our pricing: https://example.com/pricing?e={{email|url}}",
               "next": "s5"},
        "s5": {"type": "tag_contact", "tag": "lead:pricing", "next": "end"},
    },
    "reentry_policy": "skip_if_active",
}
