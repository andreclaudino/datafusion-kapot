<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Deploying a standalone kapot cluster using cargo install

A simple way to start a local cluster for testing purposes is to use cargo to install
the scheduler and executor crates.

```bash
cargo install --locked kapot-scheduler
cargo install --locked kapot-executor
```

With these crates installed, it is now possible to start a scheduler process.

```bash
RUST_LOG=info kapot-scheduler
```

The scheduler will bind to port 50050 by default.

Next, start an executor processes in a new terminal session.

```bash
RUST_LOG=info kapot-executor
```

The executor will bind to port 50051 by default. Additional executors can be started by
manually specifying a bind port. For example:

```bash
RUST_LOG=info kapot-executor --bind-port 50052
```
