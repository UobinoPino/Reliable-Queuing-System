# Edge Cases Checklist

## Single-Machine:
- [x] 1 Leader, no crash
- [x] 1 Leader, 1 Follower, no crash
- [x] 1 Leader, 2+ Followers, no crash
- [x] 1 Leader, 1 Follower, Leader crash
- [x] 1 Leader, 1 Follower, Follower crash
- [x] 1 Leader, 2+ Followers, Leader crash
- [x] 1 Leader, 2+ Followers, Follower crash
- [x] 1 Leader, 1+ Followers, Broker Addition
- [x] 1 Leader, 2+ Followers, Broker Addition after leader crash
- [x] 1 Leader, 3+ Followers, Leader + Follower simultaneous crash

## Multi-Machine:
- [x] 1 Leader, no crash
- [x] 1 Leader, 1 Follower, no crash
- [x] 1 Leader, 2+ Followers, no crash
- [x] 1 Leader, 1 Follower, Leader crash
- [x] 1 Leader, 1 Follower, Follower crash
- [x] 1 Leader, 2+ Followers, Leader crash
- [x] 1 Leader, 2+ Followers, Follower crash
- [x] 1 Leader, 1+ Followers, Broker Addition
- [x] 1 Leader, 2+ Followers, Broker Addition after leader crash

## Multi-Machine with progressive network loss:
- [x] Leader runs away
- [x] Follower runs away
- [x] Leader + Follower run away
- [x] 2 Followers run away + Leader crash