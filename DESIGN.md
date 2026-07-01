# pepperoni

The core idea is relatively simple. The current HA postgres ecosystem is all
over the place. It is my belief that every single option is trying to do too
much. There are often simpler ways to do the complex things that we would need.

Ideas that to consider:

- On postgresql service fail, we should also fail. Using systemd PartOf would
  allow for that. Allows us to start "fresh" each instance. Same instance goes
  for startup.
- Assume standby as much as possible. We do not want to be the primary, as it is
  complicated. By avoiding primary we can simplify our logic.
- Repeat re-election campaigns to ensure that we consistently have a leader.
  - Also need to make sure that there is writable node available at all times?
  - What would that operation look like?
    - Disable writes as a transaction (so any future writes bounce?)
    - New primary does pg_rewind on old primary in case.
    - New primary gets promoted and updated in etcd/haproxy.

Additional things to note:

- Will not scale well with large databases.
- This is more for small databases that need physical replication.

Potential workflow?:

- On init (before postgres start)
  - Create standby.signal (set node to standby)
  - Set primary_conninfo to empty
  - Wait for concensus
  - Determine leader
    - If no leader, start election
    - If there is a leader
      - Stop postgres
      - Create standby.signal
      - Set primary_conninfo to leader
      - Run pg_rewind
      - If pg_rewind fails, run pg_basebackup
      - Start postgres as standby

- On joining cluster
  - Wait for concensus
  - Determine leader
    - If no leader, start election
    - If there is a leader
      - Stop postgres
      - Create standby.signal
      - Set primary_conninfo to leader
      - Run pg_rewind
      - If pg_rewind fails, run pg_basebackup
      - Start postgres as standby

- On election loss (maybe will trigger our own daeomon to start and therefore
  this is irrelevant and part of the init flow)
  - Stop postgres
  - Create standby.signal
  - Set primary_conninfo to new primary
  - Run pg_rewind, and if fail pg_basebackup
  - Start postgres as standby

- On election win
  - Confirm consensus
  - Confirm leader lock
  - Confirm node is eligible
  - Promote with pg_promote
  - Announce leader in etcd

- On old primary rejoin
  - detect etcd shows a different current leader
  - Ensure standby.signal exists
  - Set primary_conninfo to current leader
  - Check pg_is_in_recovery()
    - If false
      - Stop postgres
      - pg_rewind against current leader
      - If pg_rewind fails, run pg_basebackup
      - start postgres

- On partition
  - Set node to standby
  - Wait for next cycle
