"""A task performs work and returns a JSON-serializable result."""


def greet(name: str) -> dict[str, str]:
    """Keep business logic independent of the execution envelope."""
    if not isinstance(name, str) or not name.strip() or len(name) > 200:
        raise ValueError("name must contain 1 to 200 characters and not be blank")
    return {"message": f"Hello, {name}!"}


def handle(event: dict) -> dict[str, str]:
    """CloudEvent data belongs to the application; platform IDs stay outside it."""
    return greet(event["data"]["name"])
