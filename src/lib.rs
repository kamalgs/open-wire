// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! NATS Leaf Node Gateway Server
//!
//! Provides a lightweight NATS server that accepts local client connections
//! and optionally forwards traffic to an upstream NATS hub server.

pub mod config;
pub mod nats_proto;
pub(crate) mod protocol;
pub mod server;
pub mod sub_list;
pub mod types;
pub(crate) mod upstream;
pub(crate) mod websocket;
pub(crate) mod worker;

pub use server::{
    ClientAuth, HubCredentials, LeafServer, LeafServerConfig, Permission, Permissions, UserConfig,
};
