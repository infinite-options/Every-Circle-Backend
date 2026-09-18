"""Unit tests for Connect network graph parent_uid / edges payload."""

import unittest

from network_connection import (
    _build_edges,
    _build_network_payload,
    _map_ancestor_row,
    _map_descendant_row,
    _sort_key,
    _to_response_row,
)


def _live_row(**overrides):
    base = {
        'profile_personal_uid': '110-000179',
        'profile_personal_referred_by': '110-000030',
        'profile_personal_first_name': 'Ada',
        'profile_personal_last_name': 'Lovelace',
        'profile_personal_tag_line': 'Math',
        'profile_personal_phone_number': '+15551212',
        'profile_personal_image': 'https://img/ada',
        'profile_personal_city': 'Austin',
        'profile_personal_state': 'TX',
        'user_email': 'ada@example.com',
        'user_phone_verified': 1,
        'profile_personal_email_audience': {'degree': 'all', 'circles': []},
        'profile_personal_phone_number_audience': None,
        'profile_personal_tag_line_audience': {'degree': 2, 'circles': []},
        'profile_personal_image_audience': {'degree': 'all', 'circles': []},
        'profile_personal_city_audience': {'degree': 1, 'circles': []},
        'profile_personal_state_audience': {'degree': 1, 'circles': []},
        'profile_personal_moderated': None,
        'profile_personal_is_deleted': 0,
        'circle_relationship': 'friend',
        'circle_date': '2026-09-01',
        'circle_event': 'Meetup',
        'circle_note': 'note',
        'circle_geotag': None,
        'circle_city': 'Austin',
        'circle_state': 'TX',
        'circle_introduced_by': 'Bob',
    }
    base.update(overrides)
    return base


class NetworkParentUidTests(unittest.TestCase):
    def test_descendant_parent_uid_is_db_referrer(self):
        row = _map_descendant_row(_live_row())
        self.assertEqual(row['uid'], '110-000179')
        self.assertEqual(row['parent_uid'], '110-000030')
        self.assertEqual(row['profile_personal_referred_by'], '110-000030')
        self.assertEqual(row['profile_personal_city'], 'Austin')
        self.assertTrue(row['phone_verified'])
        self.assertEqual(row['circle_introduced_by'], 'Bob')

    def test_ancestor_parent_uid_is_frontier_child(self):
        # Walking up from 110-000179 discovers parent 110-000016.
        raw = _live_row(
            profile_personal_uid='110-000179',
            profile_personal_referred_by='110-000016',
            profile_personal_first_name='Parent',
            profile_personal_last_name='Node',
        )
        row = _map_ancestor_row(raw)
        self.assertEqual(row['uid'], '110-000016')
        self.assertEqual(row['parent_uid'], '110-000179')
        # Historical inversion kept for referred_by on ancestor discovery.
        self.assertEqual(row['profile_personal_referred_by'], '110-000179')

    def test_payload_includes_degree2_edge(self):
        viewer = '110-000030'
        nodes_by_uid = {
            '110-000179': {
                **_map_descendant_row(_live_row()),
                'degree': 1,
            },
            '110-000180': {
                **_map_descendant_row(_live_row(
                    profile_personal_uid='110-000180',
                    profile_personal_referred_by='110-000179',
                    profile_personal_first_name='Grace',
                    profile_personal_last_name='Hopper',
                )),
                'degree': 2,
            },
        }
        payload = _build_network_payload(viewer, 2, nodes_by_uid, view='full')

        self.assertEqual(payload['viewer_profile_uid'], viewer)
        self.assertEqual(payload['max_degree'], 2)
        self.assertEqual(len(payload['nodes']), 2)
        self.assertEqual(payload['nodes'][0]['degree'], 1)
        self.assertEqual(payload['nodes'][0]['parent_uid'], viewer)
        self.assertEqual(payload['nodes'][0]['profile_uid'], '110-000179')
        self.assertEqual(
            payload['nodes'][0]['network_profile_personal_uid'],
            '110-000179',
        )

        self.assertEqual(payload['nodes'][1]['degree'], 2)
        self.assertEqual(payload['nodes'][1]['parent_uid'], '110-000179')

        self.assertEqual(
            payload['edges'],
            [
                {'from': viewer, 'to': '110-000179', 'degree': 1},
                {'from': '110-000179', 'to': '110-000180', 'degree': 2},
            ],
        )

        # Audience fields present for MiniCard gating.
        self.assertIn('profile_personal_email_audience', payload['nodes'][0])
        self.assertIn('profile_personal_city_audience', payload['nodes'][0])

    def test_graph_view_strips_list_fields(self):
        item = {**_map_descendant_row(_live_row()), 'degree': 1}
        row = _to_response_row('110-000030', item, view='graph')
        self.assertEqual(row['profile_uid'], '110-000179')
        self.assertEqual(row['parent_uid'], '110-000030')
        self.assertNotIn('circle_relationship', row)
        self.assertNotIn('user_email', row)
        self.assertIn('profile_personal_image', row)

    def test_sort_by_degree_then_name(self):
        items = [
            {'degree': 2, 'profile_personal_last_name': 'Zed', 'profile_personal_first_name': 'A', 'uid': '2'},
            {'degree': 1, 'profile_personal_last_name': 'Bee', 'profile_personal_first_name': 'B', 'uid': '1b'},
            {'degree': 1, 'profile_personal_last_name': 'Aye', 'profile_personal_first_name': 'A', 'uid': '1a'},
        ]
        ordered = sorted(items, key=_sort_key)
        self.assertEqual([i['uid'] for i in ordered], ['1a', '1b', '2'])

    def test_build_edges_skips_self_loops(self):
        edges = _build_edges([
            {'profile_uid': 'a', 'parent_uid': 'a', 'degree': 1},
            {'profile_uid': 'b', 'parent_uid': 'viewer', 'degree': 1},
        ])
        self.assertEqual(edges, [{'from': 'viewer', 'to': 'b', 'degree': 1}])


if __name__ == '__main__':
    unittest.main()
