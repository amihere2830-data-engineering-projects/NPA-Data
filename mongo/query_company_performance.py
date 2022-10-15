def query_company_by_quantity(performance: object, min_qty, max_qty):
    """
    Perform advanced query on companies that have quantity
    within specified range
    """
    query = {"$and": [
            {"quantiy": {"$gte": min_qty}},
            {"quantiy": {"$lte": max_qty}},
            ]}

    perf = performance.get_collection('performance')\
        .find(query).sort("quantiy")

    return perf