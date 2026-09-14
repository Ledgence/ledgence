"""An ordinary distributed task in its own immutable program package."""

def handle(event):
    pages = event["data"]["pages"]
    return {"characters": sum(len(page) for page in pages), "pages": len(pages)}
