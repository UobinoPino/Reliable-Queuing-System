# Edge Cases Checklist

## Single-Machine:
- [x] 1 Leader, no crash
- [x] 1 Leader, 1 Follower, no crash
- [x] 1 Leader, 2+ Followers, no crash
- [x] 1 Leader, 1 Follower, Leader crash
- [ ] 1 Leader, 1 Follower, Follower crash
- [x] 1 Leader, 2+ Followers, Leader crash
- [x] 1 Leader, 2+ Followers, Follower crash
- [x] 1 Leader, 1+ Followers, Broker Addition
- [x] 1 Leader, 2+ Followers, Broker Addition after leader crash
- [ ] 1 Leader, 3+ Followers, Leader + Follower simultaneous crash

## Multi-Machine:
- [ ] 1 Leader, no crash
- [ ] 1 Leader, 1 Follower, no crash
- [ ] 1 Leader, 2+ Followers, no crash
- [ ] 1 Leader, 1 Follower, Leader crash
- [ ] 1 Leader, 1 Follower, Follower crash
- [ ] 1 Leader, 2+ Followers, Leader crash
- [ ] 1 Leader, 2+ Followers, Follower crash
- [ ] 1 Leader, 1+ Followers, Broker Addition
- [ ] 1 Leader, 2+ Followers, Broker Addition after leader crash
- [ ] 1 Leader, 3+ Followers, Leader + Follower simultaneous crash

## Multi-Machine with progressive network loss:
- [ ] Leader runs away
- [ ] Follower runs away
- [ ] Leader + Follower run away
- [ ] 2 Followers run away + Leader crash