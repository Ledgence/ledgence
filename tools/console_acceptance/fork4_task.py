"""Deterministic release artifacts, without a model or external side effect (MIT)."""


def handle(event):
    data = event["data"]
    operation = data["operation"]
    if operation == "prepare":
        return {"release": data["release"], "notes": "Release notes prepared",
                "sequence": data["sequence"]}
    if operation == "publish_draft":
        return {"draft_id": "draft-" + data["prepared"]["release"],
                "prepared": data["prepared"], "checks": data["checks"]}
    if operation == "review_draft":
        return {"draft_id": data["draft"]["draft_id"], "approved": data["approve"]}
    if operation == "publish_final":
        if data["review"]["approved"] is not True:
            raise ValueError("Final publication requires approval")
        if data["review"]["draft_id"] != data["draft"]["draft_id"]:
            raise ValueError("Approval belongs to a different draft")
        return {"report_id": "report-" + data["draft"]["prepared"]["release"],
                "draft": data["draft"], "review": data["review"]}
    raise ValueError("Unknown release task operation")
