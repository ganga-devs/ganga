
# Logical operators and helpers.


def implies(p, q):
    return not p or q


def equivalent(p, q):
    return (p and q) or (not p and not q)


def xor(p, q):
    return (p and not q) or (q and not p)
