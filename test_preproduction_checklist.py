from unittest.mock import MagicMock, patch
from database import QueryCostExceededError, is_read_only_query, validate_sql_query
from athena_manager import AthenaManager

SECTION_7_SQL_TESTS = [
    ("SELECT COUNT(*) FROM cur_data", "ALLOW", True),
    ("SELECT line_item_product_code FROM cur_data", "ALLOW", True),
    (
        """WITH daily_cost AS (
            SELECT DATE(line_item_usage_start_date) AS usage_date,
                   SUM(line_item_unblended_cost) AS total_cost
            FROM cur_data GROUP BY 1
        ) SELECT usage_date, total_cost FROM daily_cost""",
        "ALLOW",
        True,
    ),
    ("SELECT * FROM cur_data", "COST_RETRY", True),
    ("SELECT c.* FROM cur_data c", "COST_RETRY", True),
    ("WITH x AS (SELECT * FROM cur_data) SELECT COUNT(*) FROM x", "COST_RETRY", True),
    ("SELECT * FROM (SELECT line_item_product_code FROM cur_data) t", "COST_RETRY", True),
    ("DROP TABLE cur_data", "IMMEDIATE_BLOCK", False),
    ("DELETE FROM cur_data", "IMMEDIATE_BLOCK", False),
    ("SELECT 1; DROP TABLE cur_data", "IMMEDIATE_BLOCK", False),
    ("SELECT 'DROP TABLE' AS message", "ALLOW", True),
    ("SELECT FROM cur_data", "SYNTAX_REPAIR", False),
]


def run_tests():
    print("=== 1. Testing Section 7 & Section 2.3 SQL Validation Suite ===")
    for sql, expected, expected_read_only in SECTION_7_SQL_TESTS:
        try:
            validate_sql_query(sql, dialect="trino")
            actual = "ALLOW"
        except PermissionError:
            actual = "IMMEDIATE_BLOCK"
        except QueryCostExceededError:
            actual = "COST_RETRY"
        except ValueError:
            actual = "SYNTAX_REPAIR"

        ro_flag = is_read_only_query(sql, dialect="trino")
        assert actual == expected, f"Mismatch on {sql!r}: expected {expected}, got {actual}"
        assert ro_flag == expected_read_only, f"is_read_only_query mismatch on {sql!r}: expected {expected_read_only}, got {ro_flag}"
        print(f"[PASS] {expected:<15} | is_read_only={str(ro_flag):<5} | {sql.splitlines()[0][:55]}")

    print("\n=== 2. Testing Fail-Closed Athena Workgroup Verification ===")
    with patch("boto3.client") as mock_boto:
        mock_athena = MagicMock()
        # Simulate missing scan cutoff on workgroup
        mock_athena.get_work_group.return_value = {
            "WorkGroup": {"Configuration": {"EnforceWorkGroupConfiguration": True}}
        }
        mock_boto.return_value = mock_athena

        try:
            AthenaManager(database="finops_db", bucket_name="my-cur-bucket", strict_workgroup_check=True)
            raise AssertionError("Expected AthenaManager to fail-closed when scan cutoff is missing!")
        except RuntimeError as e:
            print(f"[PASS] Fail-closed Workgroup check blocked startup: {e}")

    print("\nAll pre-production checks passed!")


if __name__ == "__main__":
    run_tests()
