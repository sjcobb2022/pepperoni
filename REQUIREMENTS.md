# pepperoni

## postgres requirements

Pepperoni assumes 2 major things about the postgres setup.

First, is that the postgres instance will never be queried directly. Pepperoni
acts as a proxy between a client and postgres. This allows it to control the
flow of traffic.

Second, is that postgres manages it's own replication. It is not the
responsibility of pepperoni to manage the replication of data, that is the
resposibility of the user. We will ONLY ever manage the failover of the primary
postgres server.

### pg_hba.conf

Should be configures as such:

```txt
# WAL shipping

# Other things.
```
