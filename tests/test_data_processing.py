import pandas as pd
import tempfile
import os
import sys

sys.path.insert(0, "/opt/airflow/dags")

from dags.include.data_utils import clean_appointment_data


def test_clean_appointment_data():
    # Create test data
    test_data = pd.DataFrame(
        {
            "appointment_id": [5452, 1232, 456, 4, 1234],
            "clinic_id": ["  CLINIC_A  ", "CLINIC_B", "CLiNic_c", "clinic_d", ""],
            "patient_id": [101, 102, 103, 104, 105],
            "created_at": [
                "2023-05-09 00:00",
                "2023/05/09 00:00:00",
                "2023-05-09T00:00:00",
                "invalid-date",
                "2023-05-09 00:00",
            ],
            "extra_column": ["x", "y", "z", "w", "d"],
        }
    )

    # Save to temporary CSV
    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    test_data.to_csv(temp_file.name, index=False)
    temp_file.close()

    try:
        expected_columns = ["appointment_id", "clinic_id", "patient_id", "created_at"]
        result = clean_appointment_data(temp_file.name, expected_columns)

        # Assertions
        assert len(result) == 3
        assert list(result.columns) == expected_columns
        assert result["clinic_id"].tolist() == ["clinic_a", "clinic_b", "clinic_c"]
        assert "extra_column" not in result.columns

        print("All tests passed")

    finally:
        os.unlink(temp_file.name)


if __name__ == "__main__":
    test_clean_appointment_data()
