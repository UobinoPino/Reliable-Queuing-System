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
- [ ] 1 Leader, 3+ Followers, Leader + Follower simultaneous crash *(FAILED ONCE, RETRY AGAIN)*

## Multi-Machine:
- [x] 1 Leader, no crash
- [x] 1 Leader, 1 Follower, no crash
- [x] 1 Leader, 2+ Followers, no crash
- [ ] 1 Leader, 1 Follower, Leader crash *FAILED =( v. screen Pietro*
- [x] 1 Leader, 1 Follower, Follower crash *SUCCESS ma perché il leader si ferma finché non rimuove il broker fallito?*
- [x] 1 Leader, 2+ Followers, Leader crash
- [x] 1 Leader, 2+ Followers, Follower crash
- [x] 1 Leader, 1+ Followers, Broker Addition
- [ ] 1 Leader, 2+ Followers, Broker Addition after leader crash *FAILED (client completes successfully, but the new broker is stuck without entering)*

## Multi-Machine with progressive network loss:
- [x] Leader runs away
- [x] Follower runs away
- [x] Leader + Follower run away
- [x] 2 Followers run away + Leader crash