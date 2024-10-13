from src.main import extract_to_databricks, perform_analytics


def test_main():

    res1 = extract_to_databricks()
    res2 = perform_analytics()

    assert res1, res2
