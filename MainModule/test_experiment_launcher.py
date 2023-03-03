from unittest import TestCase

from create_linking import resolve_rl_config


class Test(TestCase):
    def test_resolve_exp_config(self):
        o = resolve_rl_config({
            "exp_no": 0,
            "seed": "42",
            "l": 1024,
            "k": [9, 10, 11],
            "t": [0.7, 0.8]
        })
        e = [
            {
                "exp_no": 0,
                "seed": "42",
                "l": 1024,
                "k": 9,
                "t": 0.7
            },
            {
                "exp_no": 0,
                "seed": "42",
                "l": 1024,
                "k": 10,
                "t": 0.7
            },
            {
                "exp_no": 0,
                "seed": "42",
                "l": 1024,
                "k": 11,
                "t": 0.7
            },
            {
                "exp_no": 0,
                "seed": "42",
                "l": 1024,
                "k": 9,
                "t": 0.8
            },
            {
                "exp_no": 0,
                "seed": "42",
                "l": 1024,
                "k": 10,
                "t": 0.8
            },
            {
                "exp_no": 0,
                "seed": "42",
                "l": 1024,
                "k": 11,
                "t": 0.8
            }
        ]
        self.assertCountEqual(e, o)

        o = resolve_rl_config({
            "exp_no": 0,
            "seed": "42",
            "l": 1024,
            "k": 9,
            "t": 0.7
        })
        e = [{
            "exp_no": 0,
            "seed": "42",
            "l": 1024,
            "k": 9,
            "t": 0.7
        }]
        self.assertCountEqual(e, o)
