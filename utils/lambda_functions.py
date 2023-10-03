
coeficient = lambda w, m: int(w * m)
inc     = lambda i: i + 1
merge   = lambda c,l : f"{c}".join(l)
length  = lambda l:  len(l) > 0
zero    = lambda l:  len(l) == 0
bigger  = lambda a,  b: len(a) > b if isinstance(a, list) else  a > b
smaller = lambda a,  b: len(a) < b if isinstance(a, list) else  a < b
cols    = lambda df, x: [el for el in df.columns if el.startswith(x)]
get     = lambda a,b,m,n,l: [r for r in l if r[a] == m and r[b] == n]
