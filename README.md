# bearcats
Pandas = Bear Cats (Chinese)

# Goals

- Method chaining!
- No surprises!
- Less Googling!
- Better ergonomics (more Pythonic feel)!
- Support most common data wrangling tasks!
- Drop down to pandas when needed (pandas escape hatch)!
- Feel like the best or R/dplyr, while recognizing
- Keep dependecy to latest version of pandas

Anti-goals

- Speed is not the priority in this package
- Nor is memory efficiency
- Or 1:1 feature support
- Nothing in place. Ever!

Questions

- What to do about reindexing?
- support sql? json?
- support list of lists?
- support dictionary?
- support to_dict?
- Should I add robust typing?
- Unit Test or PyTest?
- What about single versus lists / dictionaries?
    - .select(["a"])
    - .select("a")
