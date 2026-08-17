"""Seeking-only bounty allocation: recommender 40% / network 40% / EC 20%."""

import unittest

from wallet_ids import EC_WALLET_ID
from transactions import CHARITY_PROFILE_ID, _plan_seeking_bounty_shares


def _by_profile(participants):
    return {p["tb_profile_id"]: p for p in participants}


class SeekingBountyAllocationTests(unittest.TestCase):
    def test_recommender_is_seller_one_middle_node(self):
        # Path: seller/recommender → node → buyer
        buyer, seller, node = "110-buyer", "110-seller", "110-node"
        known, network = _plan_seeking_bounty_shares(
            50.0,
            buyer,
            seller,  # recommender == seller
            f"{seller},{node},{buyer}",
            seller_id=seller,
        )
        known_map = _by_profile(known)
        network_map = _by_profile(network)

        self.assertEqual(known_map[seller]["tb_amount"], 20.0)  # 40%
        self.assertEqual(known_map[EC_WALLET_ID]["tb_amount"], 10.0)  # 20%
        self.assertNotIn(buyer, known_map)
        self.assertEqual(network_map[node]["tb_amount"], 10.0)  # 20% of pool (capped)
        self.assertEqual(network_map[CHARITY_PROFILE_ID]["tb_amount"], 10.0)

    def test_recommender_not_seller_seller_on_path(self):
        # Path: recommender → seller → buyer; seller may earn network share
        buyer, seller, recommender = "110-buyer", "110-seller", "110-rec"
        known, network = _plan_seeking_bounty_shares(
            50.0,
            buyer,
            recommender,
            f"{recommender},{seller},{buyer}",
            seller_id=seller,
        )
        known_map = _by_profile(known)
        network_map = _by_profile(network)

        self.assertEqual(known_map[recommender]["tb_amount"], 20.0)
        self.assertEqual(known_map[EC_WALLET_ID]["tb_amount"], 10.0)
        self.assertNotIn(buyer, known_map)
        self.assertNotIn(seller, known_map)
        self.assertEqual(network_map[seller]["tb_amount"], 10.0)
        self.assertEqual(network_map[CHARITY_PROFILE_ID]["tb_amount"], 10.0)

    def test_recommender_not_seller_seller_not_on_path(self):
        buyer, seller, recommender, node = (
            "110-buyer",
            "110-seller",
            "110-rec",
            "110-node",
        )
        known, network = _plan_seeking_bounty_shares(
            50.0,
            buyer,
            recommender,
            f"{recommender},{node},{buyer}",
            seller_id=seller,
        )
        known_map = _by_profile(known)
        network_map = _by_profile(network)

        self.assertEqual(known_map[recommender]["tb_amount"], 20.0)
        self.assertNotIn(seller, known_map)
        self.assertNotIn(seller, network_map)
        self.assertEqual(network_map[node]["tb_amount"], 10.0)

    def test_no_middle_nodes_network_goes_to_charity(self):
        buyer, recommender = "110-buyer", "110-rec"
        known, network = _plan_seeking_bounty_shares(
            50.0,
            buyer,
            recommender,
            f"{recommender},{buyer}",
            seller_id="110-seller",
        )
        known_map = _by_profile(known)
        network_map = _by_profile(network)

        self.assertEqual(known_map[recommender]["tb_amount"], 20.0)
        self.assertEqual(known_map[EC_WALLET_ID]["tb_amount"], 10.0)
        self.assertEqual(network_map[CHARITY_PROFILE_ID]["tb_amount"], 20.0)  # full 40%
        self.assertEqual(len(network_map), 1)

    def test_buyer_never_gets_fixed_or_middle_share(self):
        buyer, recommender = "110-buyer", "110-rec"
        # Even if buyer somehow appears mid-path, seen excludes them
        known, network = _plan_seeking_bounty_shares(
            100.0,
            buyer,
            recommender,
            f"{recommender},110-a,{buyer},110-b,{buyer}",
            seller_id="110-seller",
        )
        all_ids = {p["tb_profile_id"] for p in known + network}
        self.assertNotIn(buyer, all_ids)

    def test_three_middle_nodes_split_pool_evenly_under_cap(self):
        buyer, rec = "110-buyer", "110-rec"
        middles = ["110-a", "110-b", "110-c"]
        path = ",".join([rec] + middles + [buyer])
        _known, network = _plan_seeking_bounty_shares(
            100.0, buyer, rec, path, seller_id="110-seller"
        )
        network_map = _by_profile(network)
        # pool=40, max_per=20 → per_person = min(40/3, 20) ≈ 13.3333
        expected = round(40 / 3, 4)
        for uid in middles:
            self.assertEqual(network_map[uid]["tb_amount"], expected)
        paid = sum(network_map[u]["tb_amount"] for u in middles)
        charity = network_map.get(CHARITY_PROFILE_ID, {}).get("tb_amount", 0.0)
        self.assertAlmostEqual(paid + charity, 40.0, places=3)

    def test_two_middle_nodes_hit_20_pct_cap_with_charity_excess(self):
        # Cap case: 1 middle would get min(40, 20)=20 + charity 20;
        # with 2 middles each get min(20, 20)=20, charity 0
        buyer, rec = "110-buyer", "110-rec"
        path = f"{rec},110-a,110-b,{buyer}"
        _known, network = _plan_seeking_bounty_shares(
            100.0, buyer, rec, path, seller_id="110-seller"
        )
        network_map = _by_profile(network)
        self.assertEqual(network_map["110-a"]["tb_amount"], 20.0)
        self.assertEqual(network_map["110-b"]["tb_amount"], 20.0)
        self.assertNotIn(CHARITY_PROFILE_ID, network_map)


if __name__ == "__main__":
    unittest.main()
