# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Composition
from materialize.mzcompose.services import (
    Loki,
    Materialized,
    Testdrive,
)

SERVICES = [
    Loki(),
    Materialized(),
    Testdrive(loki_addr="http://loki:3100"),
]


def workflow_default(c: Composition) -> None:
    c.start_and_wait_for_tcp(services=["loki"])
    c.up("materialized")
    c.wait_for_materialized("materialized")
    c.run("testdrive-svc", "loki.td")
    c.rm("loki", "materialized", destroy_volumes=True)
