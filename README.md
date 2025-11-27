# Flipcash Server

[![Release](https://img.shields.io/github/v/release/code-payments/flipcash2-server.svg)](https://github.com/code-payments/flipcash2-server/releases/latest)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/code-payments/flipcash2-server)](https://pkg.go.dev/github.com/code-payments/flipcash2-server)
[![Tests](https://github.com/code-payments/flipcash2-server/actions/workflows/test.yml/badge.svg)](https://github.com/code-payments/flipcash2-server/actions/workflows/test.yml)
[![GitHub License](https://img.shields.io/badge/license-MIT-lightgrey.svg?style=flat)](https://github.com/code-payments/flipcash2-server/blob/main/LICENSE.md)

Flipcash server monolith containing the gRPC/web services and workers that power Flipcash.

## What is Flipcash?

[Flipcash](https://flipcash.com) is a mobile wallet app leveraging self-custodial blockchain technology to provide an instant, global, and private payments experience. We are currently working on a currency launchpad.

## Quick Start

1. Install Go. See the [official documentation](https://go.dev/doc/install).

2. Download the source code.

```bash
git clone git@github.com:code-payments/flipcash2-server.git
```

3. Run the test suite:

```bash
make test
```
