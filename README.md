# pepperoni

WIP

a postgres failover service, like patroni, but hot.

this is both a library and a binary. it is extremely opinionated as to how
leader election etc should be done.

the library is dependency free, `no_std`, no alloc, and sans-io. the user is
encouraged to provide their own runtime.

the core binary uses tokio, etcd, and pgctl commands.

in the future there will be examples using other async runtimes and various
other fun things. perhaps some of these features will be introduced into the
main binary with some dynamic dispatch.

> [!WARNING]
> This is a learning project and (probably...) should not be used.

Made with love and without AI.
