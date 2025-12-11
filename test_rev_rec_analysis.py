import unittest
from rev_rec_analysis import to_owner_record, to_reviewer_records
from typing import Dict, List
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline

class TestRevRecPipeline(unittest.TestCase):

    def test_to_owner_record_complete(self):
        """Test extraction of owner data when all fields are present."""
        change = {
            "owner": {
                "accountId": "123",
                "email": "owner@example.com",
                "name": "OwnerName"
            },
            "timestamp": 1234567890
        }
        key, value = to_owner_record(change)
        self.assertEqual(key, "123")
        self.assertEqual(value['accountId'], "123")
        self.assertEqual(value['email'], "owner@example.com")
        self.assertEqual(value['name'], "OwnerName")

    def test_to_owner_record_missing_email(self):
        """Test extraction when email is missing."""
        change = {
            "owner": {
                "accountId": "123",
                "name": "OwnerName"
            }
        }
        key, value = to_owner_record(change)
        self.assertEqual(value['email'], "")  # email should default to empty string

    def test_to_owner_record_none_owner(self):
        """Test extraction when owner is None."""
        change = {"owner": None}
        key, value = to_owner_record(change)
        self.assertEqual(key, "unknown")
        self.assertEqual(value['email'], "")
        self.assertEqual(value['name'], "")

    def test_to_reviewer_records_complete(self):
        """Test extraction of reviewer data with all fields."""
        change = {
            "reviewers": [
                {"accountId": "r1", "email": "r1@example.com", "name": "Reviewer1"},
                {"accountId": "r2", "email": "r2@example.com", "name": "Reviewer2"}
            ],
            "timestamp": 1234567890
        }
        records = list(to_reviewer_records(change))
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0][0], "r1")
        self.assertEqual(records[0][1]['email'], "r1@example.com")
        self.assertEqual(records[1][0], "r2")
        self.assertEqual(records[1][1]['name'], "Reviewer2")

    def test_to_reviewer_records_missing_fields(self):
        """Test reviewer extraction with missing fields."""
        change = {
            "reviewers": [{"accountId": None, "email": None, "name": None}]
        }
        records = list(to_reviewer_records(change))
        self.assertEqual(records[0][0], "unknown")
        self.assertEqual(records[0][1]['email'], "")
        self.assertEqual(records[0][1]['name'], "")

    def test_to_reviewer_records_none_list(self):
        """Test reviewer extraction when reviewers list is None."""
        change = {"reviewers": None}
        records = list(to_reviewer_records(change))
        self.assertEqual(records, [])

    def test_owner_pipeline(self):
        """Test Beam pipeline for extracting owners."""
        sample_data = [
            {"owner": {"accountId": "1", "email": "a@example.com", "name": "Alice"}},
            {"owner": {"accountId": "2", "email": "b@example.com", "name": "Bob"}},
            {"owner": None}  # test None handling
        ]

        expected_output = [
            ("1", {'accountId': "1", 'email': "a@example.com", 'name': "Alice"}),
            ("2", {'accountId': "2", 'email': "b@example.com", 'name': "Bob"}),
            ("unknown", {'accountId': None, 'email': "", 'name': ""})
        ]

        with TestPipeline() as p:
            owners = (
                p
                | "Create sample data" >> beam.Create(sample_data)
                | "Extract owners" >> beam.Map(to_owner_record)
            )

            assert_that(owners, equal_to(expected_output))

    def test_reviewer_pipeline(self):
        """Test Beam pipeline for extracting reviewers."""
        sample_data = [
            {"reviewers": [
                {"accountId": "r1", "email": "r1@example.com", "name": "Rev1"},
                {"accountId": "r2", "email": "r2@example.com", "name": "Rev2"}
            ]},
            {"reviewers": None}  # test None handling
        ]

        expected_output = [
            ("r1", {'accountId': "r1", 'email': "r1@example.com", 'name': "Rev1", 'timestamp': None}),
            ("r2", {'accountId': "r2", 'email': "r2@example.com", 'name': "Rev2", 'timestamp': None})
        ]

        with TestPipeline() as p:
            reviewers = (
                p
                | "Create sample reviewer data" >> beam.Create(sample_data)
                | "Extract reviewers" >> beam.FlatMap(to_reviewer_records)
            )

            assert_that(reviewers, equal_to(expected_output))

if __name__ == "__main__":
    unittest.main()
