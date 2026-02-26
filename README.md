# Norn

Norn is an experimental set of libraries for building single-threaded asynchronous
runtimes.

## What Itch Does It Scratch?

Norn is designed for applications which meet two criteria:

1. Fundementally I/O bound
2. Trivially shardable or non-parallizable.

## Why use Norn over X?

You probably should not. Norn is not a general purpose runtime. It's meant
for very specific workloads (sharded I/O bound storage systems).

## Status of the Project

Norn is still in the early stages of development. The API is still in flux
and in most cases non-existant.

- [`norn-task`] is the core task system. It is mostly complete. I don't envision
  any substantial changes to the API.
- [`norn-executor`] is the single-threaded executor. It is not complete. The
  API is likely to change.
- [`norn-nursery`] provides scoped async concurrency on top of `norn-task`.
  It is inspired by [moro].
- [`norn-uring`] is a uring-based backend for the executor. It is not complete
  and hardly useful. The API is very likely to change.

## Design Inspo

Much of the design of the task system and async submission handling was inspired
by Tokio and tokio-uring. The general approach to handling tasks is very similar
in that we use a single allocation per task, and track tasks in a linked list
for easy shutdown.

[moro]: https://github.com/nikomatsakis/moro
