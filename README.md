# What's This

Toy Lock-free Payment System Using [materialize](https://github.com/MaterializeInc/materialize)

# How to Run

```bash
# run materialize db
docker-compose up

# run migration script
cargo run --bin migrate

cargo run
```

You can create transactions by making POST requests to `http://localhost:3000/transactions` with the following JSON body:

```json
{
  "wallet_id": 1,
  "amount": 100,
  "transaction_id": "<you should verify transaction_id is auto_incremented value>"
}
```

Also, you can run `gen_tran` binary to generate transactions randomly.

```bash
# generate 100 transactions to wallet 1 with amount 1: charge
cargo run --bin gen_tran 100 1 1

# you can send request with negative amount: payment
cargo run --bin gen_tran 100 1 -1

# this system should not allow to create payment transaction exceeding wallet balance

# wallet balance is now 100
cargo run --bin gen_tran 100 1 1


# the last transaction should be rejected and wallet balance should be 0
cargo run --bin gen_tran 101 1 -1
```

# TODO

- handle errors and re-execute `BEGIN` statement when stream transaction goes to stale
- handle all executing requests in the `responder_hashmap` before shutting down the server
- use connection_pool instead of single db connection
- sweep out the old stale requests from `responder_hasmap` to keep the memory usage low
