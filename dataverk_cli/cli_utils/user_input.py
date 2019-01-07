

AFFIRMATIVE = ('j', 'ja', 'y', 'yes')
NEGATIVE = ('n', 'nei', 'no')


def cli_question(message: str) -> bool:
    res = input(message)
    if res.lower() in AFFIRMATIVE:
        return True
    return False
