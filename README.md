# cured-rs

WIP

a collection of crates used to handle postgres failover.

[pepperoni](./crates/pepperoni): core binary, like patroni but hot.

[salami](./crates/salami): library that drives pepperoni.

salami library is dependency free, `no_std`, no alloc, and sans-io. the user is
encouraged to provide their own runtime, or use pepperoni.

pepperoni uses tokio, etcd, and pgctl commands.

in the future there will be examples using other async runtimes and various
other fun things. perhaps some of these features will be introduced into the
main binary with some dynamic dispatch.

uses [crane](https://github.com/ipetkov/crane) for the rust flake template.

> [!WARNING]
> This is a learning project and (probably...) should not be used.

Made with love and without AI.
