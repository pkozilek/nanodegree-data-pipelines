test_cases = [
    {
        "name": "Check for null userids",
        "query": "SELECT count(*) FROM users WHERE userid IS NULL",
        "expected_result": 0,
    },
    {
        "name": "Check for null playids",
        "query": "SELECT count(*) FROM songplays WHERE playid IS NULL",
        "expected_result": 0,
    },
]
