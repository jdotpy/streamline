from test_e2e import do_cli_call


def test_appending_data():
    do_cli_call(
        ' -- '.join([
            'streamline json history:push extract py extract history:pop combine extract',
            '--selector "number"',
            '\'{ "value": value, "new": value * 8 }\'',
            '--selector "new"',
            '--path result',
            '--selector result',
        ]),
        '{"number": 1}\n{"number": 2}\n{"number": 3}',
        '8\n16\n24',
    )

def test_shorthand_simple():
    do_cli_call(
        ' -- '.join([
            'streamline json py(number,new)',
            '\'{ "value": value, "new": value * 8 }\'',
        ]),
        '{"number": 1}\n{"number": 2}\n{"number": 3}',
        '8\n16\n24',
    )

def test_shorthand_assignment():
    do_cli_call(
        ' -- '.join([
            'streamline json foobar=py(number,new) extract',
            '\'{ "value": value, "new": value * 8 }\'',
            '--selector foobar',
        ]),
        '{"number": 1}\n{"number": 2}\n{"number": 3}',
        '8\n16\n24',
    )
