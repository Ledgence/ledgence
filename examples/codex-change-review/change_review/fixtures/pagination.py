"""Page count for document search results, with 100 results per page."""


def page_count(item_count):
    if type(item_count) is not int:
        raise TypeError("item_count must be an integer")
    if item_count < 0:
        raise ValueError("item_count must be nonnegative")
    return item_count // 100 + 1
