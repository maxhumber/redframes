# bearcats
Pandas = Bear Cats (Chinese)

### Goals

- Method chaining!
- No surprises!
- Less Googling!
- Better ergonomics (more Pythonic feel)!
- Support most common data wrangling tasks!
- Drop down to pandas when needed (pandas escape hatch)!
- Feel like the best or R/dplyr, while recognizing
- Keep dependecy to latest version of pandas
- one file (for auditability)
- everything is a verb
- escape hatching!!

### Anti-goals

- Speed is not the priority in this package
- Nor is memory efficiency
- Or 1:1 feature support
- Nothing in place. Ever!

### Questions

- Should there be a convert/revert... or just sit it on top of pandas?
- support sql? json?
- support list of lists?
- support dictionary?
- support to_dict?
- Should I add robust typing? Yes...


### Decisions
- Will write unittests
- No info, dimensions, shape, describe... if you need that, use pandas
    - This is a pipelining transformation tool
    - not for exploration
- `.mutate(f=lambda d: d["e"] / 5)` is no good. Use a dictionary!
- `.select("a")` is no good. Use a list!
- reindex is fine
