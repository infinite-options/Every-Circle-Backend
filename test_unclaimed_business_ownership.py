"""Unit tests for unclaimed business ownership helpers."""

import unittest

from business_info import _is_unclaimed_business_role


class UnclaimedBusinessRoleTests(unittest.TestCase):
    def test_unclaimed_tokens(self):
        for role in (None, "", "  ", "unclaimed", "Unclaimed", "UNCLAIMED", "null", "none", "n/a"):
            self.assertTrue(_is_unclaimed_business_role(role), role)

    def test_real_roles(self):
        for role in ("owner", "co_owner", "manager", "employee", "Owner"):
            self.assertFalse(_is_unclaimed_business_role(role), role)


if __name__ == "__main__":
    unittest.main()
